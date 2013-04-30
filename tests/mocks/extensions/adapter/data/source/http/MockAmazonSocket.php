<?php
/**
 * Lithium: the most rad php framework
 *
 * @copyright     Copyright 2012, Union of RAD (http://union-of-rad.org)
 * @license       http://opensource.org/licenses/bsd-license.php The BSD License
 */

namespace li3_aws\tests\mocks\extensions\adapter\data\source\http;

class MockAmazonSocket extends \lithium\net\Socket {

	protected $_response = null;
	public static $data = '';
	protected $_fp;
	public static $buckets = array();
	public static $temp = array();
	public static $requests;
	
	protected function _insert($host, $path = null, array $data = array()) {
		$path = ltrim($path, '/');
		$chunks = explode('.', $host);
		$chunks = array_reverse($chunks);
		$buckets = &static::$buckets;
		foreach ($chunks as $chunk) {
			if (!array_key_exists($chunk, $buckets)) {
				$buckets[$chunk] = array();
				$buckets['/'] = 'host';
			}
			$buckets = &$buckets[$chunk];
		}
		$buckets['/'] = 'bucket';
		if (!empty($path)) {
			$buckets[$path] = $data;
		}
		return $buckets;
	}

	protected function _delete($host, $path = null) {
		$path = ltrim($path, '/');
		$chunks = explode('.', $host);
		$chunks = array_reverse($chunks);
		$buckets = &static::$buckets;
		foreach ($chunks as $chunk) {
			if (!array_key_exists($chunk, $buckets)) {
				return false;
			}
			$buckets = &$buckets[$chunk];
		}
		if (empty($path)) {
			unset($buckets);
		} else {
			unset($buckets[$path]);
		}
		return true;
	}
	
	protected function _find($host, $path = null) {
		$path = ltrim($path, '/');
		$chunks = explode('.', $host);
		$chunks = array_reverse($chunks);
		if (!empty($path)) {
			$chunks[] = $path;
		}
		$buckets = static::$buckets;
		foreach ($chunks as $chunk) {
			if (array_key_exists($chunk, $buckets)) {
				$buckets = $buckets[$chunk];
			} else {
				return null;
			}
		}
		return $buckets;
	}
	
	public function open(array $options = array()) {
		parent::open($options);
		$this->_fp = fopen("php://temp", 'w');
		$data = $options['message'];
		if (empty(static::$data)) {
			$url = $data->to('url');
			$path = parse_url($url);
			if (array_key_exists('path', $path)) {
				$result = $this->_find($path['host'], $path['path']);
				fwrite($this->_fp, (isset($result['body'])) ? $result['body'] : '');
			}
		} else {
			fwrite($this->_fp, static::$data);
		}
		rewind($this->_fp);
		return $this->_fp;
	}

	public function close() {
		$res = (!empty($this->_fp)) ? fclose($this->_fp) : true;
		$this->_fp = null;
		return $res;
	}

	public function eof() {
		return feof($this->_fp);
	}

	public function read() {
		$data = static::$data;
		static::$data = null;
		return (empty($data)) ? $this->_response : $data;
	}

	public static function resetData() {
		static::$buckets = array();
		static::$data = '';
		static::$requests = array();
	}


	public function write($data) {
		$url = $data->to('url');
		$path = parse_url($url);
		static::$requests[] = $data;
		$requests = static::$requests;
		$data->body = implode('', (array)$data->body);
		$response = $this->_instance('response');
		switch ($data->method) {
			case 'PUT' :
			case 'POST' :
				$response->headers += array(
					'x-amz-id-2' => 'foo',
					'x-amz-request-id' => 'bar',
					'Date' => 'Fri, 02 Dec 2011 01:53:42 GMT',
				);
				//delete multiple objects
				if (array_key_exists('query', $path) && $path['query'] == 'delete') {
					$xml = simplexml_load_string($data->body);
					foreach($xml->children() as $object) {
						$test = (string)$object;
						$this->_delete($path['host'], $object->Key);
					}
					$result = simplexml_load_string('<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>');
					$response->body = $result->asXML();
					$response->headers += array(
						'Content-Type' => 'application/xml',
						'Content-Length' => strlen($response->body),
					);
				// multipart upload Init
				} else if (array_key_exists('query', $path) && $path['query'] == 'uploads') {
					$bucket = explode('.', $path['host']);
					static::$temp = array(
						'UploadId' => 'foo',
						'Key'      => ltrim($path['path'], '/'),
						'Bucket'   => $bucket[0],
					);
					$xml = simplexml_load_string('<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></InitiateMultipartUploadResult>');
					foreach(static::$temp as $key => $val) {
						$xml->addChild($key, $val);
					}
					$response->body = $xml->asXML();
					$response->headers += array(
						'Content-Type' => 'application/xml',
						'Content-Length' => strlen($response->body),
					);
				//multipart upload chunk
				} else if (array_key_exists('query', $path) && strpos($path['query'], 'uploadId') !== false && strpos($path['query'], 'partNumber') !== false) {
					parse_str($path['query'], $query);
					if ($query['uploadId'] != static::$temp['UploadId']) {
						$response->status['code'] = 404;
					} else {
						static::$temp['data'][$query['partNumber']] = $data->body;
						$response->headers('ETag', $query['uploadId'].$query['partNumber']);
					}
				//multipart upload complete
				} else if (array_key_exists('query', $path) && strpos($path['query'], 'uploadId') !== false) {
					parse_str($path['query'], $query);
					if ($query['uploadId'] != static::$temp['UploadId']) {
						$response->status['code'] = 404;
					} else {
						$xml = simplexml_load_string($data->body);
						$success = $xml->getName() == 'CompleteMultipartUpload';
						$combinedData = '';
						foreach ($xml as $part) {
							$num = (int)$part->PartNumber;
							$success = $success && isset(static::$temp['data'][$num]);
							$success = $success && $part->ETag == static::$temp['UploadId'].$num;
							if ($success) {
								$combinedData .= static::$temp['data'][$num];
							}
						}
						$success = $success && $this->_insert($path['host'], $path['path'], array('headers' => $data->headers, 'body' => $combinedData));
						if ($success) {
							$xml = simplexml_load_string('<CompleteMultipartUploadResult></CompleteMultipartUploadResult>');
							$response->body = $xml->asXML();
						} else {
							$response->status['code'] = 404;
							$xml = simplexml_load_string('<Error></Error>');
							$xml->addChild('Code', 'InvalidPart');
							$xml->addChild('Message', 'InvalidPart');
							$response->body = $xml->asXML();
						}
					}
				} else {
					$this->_insert($path['host'], $path['path'], array('headers' => $data->headers, 'body' => $data->body));
				}
				break;
			case 'GET':
				$body = '';
				$result = $this->_find($path['host'], $path['path']);
				$response->status('code', $result ? 200 : 404);
				if ($response->status['code'] == 200) {
					if (array_key_exists('/', $result)) {
						$response->headers('Content-Type', 'application/xml');
						if ($result['/'] == 'bucket') {
							$xml = simplexml_load_string('<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></ListBucketResult>');
							unset($result['/']);
							foreach ($result as $key => $entry) {
								$content = $xml->addChild('Contents');
								$content->addChild('Key', $key);
							}
						} else {
							$xml = simplexml_load_string('<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></ListAllMyBucketsResult>');
							unset($result['/']);
							foreach ($result as $key => $entry) {
								$buckets = $xml->addChild('Buckets');
								$bucket = $buckets->addChild('Bucket');
								$bucket->addChild('Name', $key);
							}
						}
						$body = $xml->asXML();
					} else {
						$response->headers = $result['headers'];
						$body = $result['body'];
					}
					$response->headers('Content-Length', strlen($body));
					$response->body = $body;
				}
				break;
			case 'HEAD': 
				$result = $this->_find($path['host'], $path['path']);
				$response->status('code', $result ? 200 : 403);
				break;
			case 'DELETE':
				$result = $this->_delete($path['host'], $path['path']);
				$response->status('code', $result ? 204 : 403);
		}
		$this->_response = $response;
		return $response;
	}

	public function timeout($time) {
		return true;
	}

	public function encoding($charset) {
		return true;
	}
}

?>