<?php

namespace li3_aws\extensions\adapter\data\source\http;

use lithium\net\http\Media;
use lithium\util\String;
use lithium\util\Inflector;
use lithium\data\model\QueryException;
use li3_aws\data\AmazonS3File;

class AmazonS3 extends \lithium\data\source\Http {

	protected static $CHUNK_SIZE = 5242880; //5MB
	
	protected $_classes = array(
		'service' => 'lithium\net\http\Service',
		'entity'  => 'lithium\data\entity\Document',
		'set'     => 'lithium\data\collection\DocumentSet',
		'schema'  => 'lithium\data\DocumentSchema'
	);	
	
	protected $_sources = array(
		'paths' => array(
			'create' => array(
				'{:source}.s3.{:host}/' => array('source'),
				'{:source}.s3.{:host}/{:object}' => array('source', 'object'),
			),
			'delete' => array(
				'{:source}.s3.{:host}/' => array('source'),
				'{:source}.s3.{:host}/?{:context}' => array('source', 'context'),
				'{:source}.s3.{:host}/{:object}' => array('source','object'),
				'{:source}.s3.{:host}/?{:context}' => array('source','object', 'context'),
			),
			'read' => array(
				's3.{:host}/' => array(),
				'{:source}.s3.{:host}/' => array('source'),
				'{:source}.s3.{:host}/?{:context}' => array('source', 'context'),
				'{:source}.s3.{:host}/{:object}' => array('source','object'),
				'{:source}.s3.{:host}/{:object}?{:context}' => array('source','object', 'context'),
			),
		),
		'responseFilters' => array(
			'Buckets/Bucket',
			'Contents',
			'Error',
		),
	);

	protected $_xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";
	
	protected $_regions = array(
		'EU',
		'eu-west-1', 
		'us-west-1', 
		'us-west-2',
		'ap-southeast-1',
		'ap-northeast-1',
		'sa-east-1',
	);
	
	public function __construct(array $config = array()) {
		$defaults = array(
			'scheme'   => 'http',
			'host'     => 'amazonaws.com',
			'port'     => 80,
			'key'      => null,
			'secret'   => null,
			'bucket'   => null,
			'version'  => '1.1',
		);
		$config += $defaults;
		parent::__construct($config + $defaults);
	}	
	
	protected function _encode($param) {
		$result = '';
		$param = (array)$param;
		foreach ((array)$param as $key => $value) {
			$result .= '&' . ((is_int($key)) ? urlencode($value) : http_build_query(array($key => $value)));
		}
		return ltrim($result, '&');	
	}
	
	protected function _path($type, array $params) {
		if (!isset($this->_sources['paths'][$type])) {
			return null;
		}
		$host = $this->_config['host'];
		$keys = array_keys($params);
		sort($keys);
		foreach ($this->_sources['paths'][$type] as $path => $required) {
			sort($required);

			if ($keys !== $required) {
				continue;
			}
			$params = array_map(array($this, '_encode'), (array) $params);
			$path = String::insert($path, $params + compact('host'));
			$pos = strpos($path, '/');
			return array(
				'host' => substr($path, 0, $pos),
				'path' => substr($path, $pos),
			);
		}
	}
	
	protected function _request($query, array $pathConfig, array $options = array()) {
		$defaults = array(
			'return' => 'response',
		);
		$options += $defaults;
		$defaults = array(
			'body' => '',
		);
		$pathConfig += $defaults;
		$return = $options['return'];
		$headers = $this->_requestHeaders($pathConfig, $options);
		$method = $pathConfig['method'];
		$config = compact('headers', 'return') + array('host' => $pathConfig['host'], 'type' => null);
		if (array_key_exists('type', $pathConfig) && $pathConfig['type'] !== null) {
			//if 
			$config['type'] = Media::type($pathConfig['type']);
			//if no Media type found, set request _type flag to content-type and type flag to null, to pass Media handler 
			if (!$config['type']) {
				$config['_type'] = $pathConfig['type'];
			}
		}
		switch ($return) {
			case ('stream'): 
				$conn = $this->connection;	
				$request = $conn->invokeMethod('_request', array($method, $pathConfig['path'], array(), $config));
				$config += array('message' => $request);
				$result = $conn->connection->open($config);
				$meta = stream_get_meta_data($result);
				$headers = isset($meta['wrapper_data'])	? join("\r\n", $meta['wrapper_data']) . "\r\n\r\n" : null;
				$response = $conn->invokeMethod('_instance', array('response', array('message' => $headers)));
				$fileParams = array(
					'stream'   => $result,
					'filename' => $pathConfig['object'],
					'size'     => $response->headers('Content-Length'),
				);
				$response->body = new AmazonS3File($fileParams);
				break;
			default:
				$response = $this->connection->send($method, $pathConfig['path'], $pathConfig['body'], $config);
		}
		$statusCode = ($response) ? $response->status['code'] : '409'; // set to 409 Conflict, if no response
		if ($statusCode != '200' && $statusCode != '204') {
			$entity = $query->entity();
			if ($response && ($type = $response->headers('Content-Type')) && strpos($type, 'xml') && $entity) {
				$xml = simplexml_load_string($response->body);
				if ($xml) {
					$entity->errors($pathConfig['source'], (string)$xml->Message);
				}
			}
			return false;
		}
		return $response;
	}
	
	protected function _multipartUpload($query, array $pathConfig, array $options = array()) {
		$defaults = array(
			'retry'      => 1,
			'chunk_size' => static::$CHUNK_SIZE,
		);
		$options += $defaults;
		$file = $pathConfig['body'];
		$options['md5'] = null; //md5 will be calculated for every chunk
		$eTags = array();
		$initConfig = array(
			'method'  => 'POST',
			'body'    => '',
			'context' => array("uploads"),
			'size'    => null,
			'path'    => "{$pathConfig['path']}?uploads",
		) + $pathConfig;
		$closeConfig = null;
		$fh = fopen($file, 'r');
		//init multipart upload
		if ($initResponse = $this->_request($query, $initConfig, $options)) {
			unset($options['encryption']); //remove encryption setup on uploading parts
			$xml = simplexml_load_string($initResponse->body);
			$uploadId = (string)$xml->UploadId;
			$uploadConfig = array(
				'method'  => 'PUT',
				'context' => compact('uploadId'),
			) + $pathConfig;
			//upload parts
			$chunkSize = $options['chunk_size'];
			$partNumber = 1;
			$eof = false;
			while (!$eof) {
				$body = fread($fh, $chunkSize);
				$eof = feof($fh);
				$data = compact('body') + $uploadConfig;
				$data['context'] += compact('partNumber');
				$data['type'] = 'application/octet-stream';
				$data['size'] = ($eof) ? strlen($body) : $chunkSize;
				$data['path'] = "{$pathConfig['path']}?{$this->_encode($data['context'])}";
				$uploadResponse = null;
				for ($i=0; !$uploadResponse && $i < $options['retry']; $i++) {
					$uploadResponse = $this->_request($query, $data, $options);
				}
				if (!$uploadResponse) {
					return false;
				}
				$eTags[] = $uploadResponse->headers('ETag');
				$partNumber++;
			}
			$body = simplexml_load_string('<CompleteMultipartUpload></CompleteMultipartUpload>');
			for($i=1; $i<$partNumber; $i++) {
				$part = $body->addChild('Part');
				$part->addChild('PartNumber', $i);
				$part->addChild('ETag', $eTags[$i-1]);
			}
			//close multipart upload
			$closeConfig = array(
				'body'    => $body->asXML(),
				'type'    => 'application/xml',
				'context' => compact('uploadId'),
				'path'    => "{$pathConfig['path']}?{$this->_encode(compact('uploadId'))}",
			) + $initConfig;
			if ($response = $this->_request($query, $closeConfig, $options)) {
				$entity = $query->entity();
				if (($type = $response->headers('Content-Type')) && strpos($type, 'xml') && $entity) {
					$xml = simplexml_load_string($response->body);
					if ($xml->getName() == 'Error') {
						$entity->errors($pathConfig['source'], (string)$xml->Message);
						return false;
					}
				}
			}
		}
		fclose($fh);
		return $response;
	}
	
	protected function _multipleRequest($query, array $pathConfig, array $options = array()) {
		$responses = array();
		foreach ($pathConfig['body'] as $requestBody) {
			$pathConfig['body'] = $requestBody;
			$responses[] = $this->_request($query, $pathConfig, $options);
 		}
		return $responses;
	}
	
	protected function _requestHeaders(array $params, array $options = array()) {
		$defaults = array(
			'x-amz'   => array(),
			'type'    => null,
			'method'  => 'GET',
			'body'    => '',
			'size'    => null,
			'source'  => '',
			'object'  => '',
			'context' => array(),
			'host'    => $this->_config['host'],
		);
		$params += $defaults;
		$headers = array();
		$headers['Host'] = $params['host'];
		$amz_headers = $params['x-amz'];
		$date = '';
		if (!array_key_exists('x-amz-date', $amz_headers)) {
			$date = gmdate('r'); // GMT based timestamp
			$headers['Date'] = $date;
		}
		if (isset($options['encryption'])) {
			$amz_headers['x-amz-server-side-encryption'] = $options['encryption'];
		}
		//add amz headers
		$headers += $amz_headers;
		arsort($amz_headers);
		$canonicalizedAmzHeaders = "";
		foreach ($amz_headers as $key => $val) {
			$key =  strtolower($key);
			$canonicalizedAmzHeaders .= "{$key}:{$val}\n";
		}
		if ($params['type']) {
			$headers['Content-Type'] = $params['type'];
		}
		$headers['Content-Length'] = $params['size'] ?: strlen($params['body']);
		$contentMD5 = ''; 
		if ($params['body']) {
			$base64MD5 = function($md5) {
				return array_reduce(str_split($md5, 2), function($res, $val) {
					return $res .= chr(hexdec($val));
				}, '');
			};
			$contentMD5 = (isset($options['md5'])) ? $base64MD5($options['md5']) : md5($params['body'], true);
			$contentMD5 = base64_encode($contentMD5);
			$headers['Content-MD5'] = $contentMD5;
		}
		
		$canonicalizedResource = (empty($params['source'])) ? "" : "/{$params['source']}";
		$canonicalizedResource.= "/{$params['object']}";
		if (!empty($params['context'])) {
			$context = explode('&', $this->_encode($params['context']));
			sort($context);
			$canonicalizedResource .= '?' . implode('&', $context);
		}
		// preparing string to sign
		$string2sign = "{$params['method']}\n{$contentMD5}\n{$params['type']}\n{$date}\n{$canonicalizedAmzHeaders}{$canonicalizedResource}";
		$aws_secret = $this->_config['secret'];
		//assuming key is global $aws_secret 40 bytes long
		$aws_secret = (strlen($aws_secret) == 40) ? $aws_secret.str_repeat(chr(0), 24) : $aws_secret;
		$ipad = str_repeat(chr(0x36), 64);
		$opad = str_repeat(chr(0x5c), 64);
		$hmac = sha1(($aws_secret^$opad).sha1(($aws_secret^$ipad).$string2sign, true), true);
		$signature = base64_encode($hmac);
		$headers['Authorization'] = "AWS {$this->_config['key']}:{$signature}";
		return $headers;
	}

	protected function _filterResponse($xml) {
		$ns = '';
		$namespaces = $xml->getDocNamespaces();
		if (array_key_exists('', $namespaces)) {
			$xml->registerXPathNamespace('ns', $namespaces['']);
			$ns = 'ns:';
		}
		foreach ($this->_sources['responseFilters'] as $filter) {
			$test = $ns . str_replace('/', "/{$ns}", $filter);
			if ($result = $xml->xpath($ns . str_replace('/', "/{$ns}", $filter))) {
				return $result;
			}
		}
		return null;
	}

	/**
	 * Formats a S3 result set into a standard result to be passed to item.
	 *
	 * @param array $data data returned from query
	 * @return array
	 */
	protected function _format($data, $key = null) {
		if (isset($data['key'])) {
			$data[$key] = $data['key'];
			unset($data['key']);
		}
		if (isset($data['name'])) {
			$data['source'] = $data['name'];
			unset($data['name']);
		}
		return $data;
	}
	
	protected function _simpleXml2Array($xml = null) {
		if ($xml === null) {
			return array();
		}
		if (is_object($xml) && get_class($xml) == 'SimpleXMLElement') {
			$attributes = $xml->attributes();
			foreach($attributes as $k=>$v) {
				if ($v) $a[strtolower($k)] = (string) $v;
			}
			$x = $xml;
			$xml = get_object_vars($xml);
		}
		if (is_array($xml)) {
			if (count($xml) == 0) return (string) $x; // for CDATA
			foreach($xml as $key=>$value) {
				$r[strtolower($key)] = $this->_simpleXml2Array($value);
			}
			if (isset($a)) $r['@attributes'] = $a;    // Attributes
			return $r;
		}
		return (string) $xml;
	}
	
	/**
	 * Configures a model class by setting the primary key to `'_id'`.
	 *
	 * @see lithium\data\Model::$_meta
	 * @see lithium\data\Model::$_classes
	 * @param string $class The fully-namespaced model class name to be configured.
	 * @return Returns an array containing keys `'classes'` and `'meta'`, which will be merged with
	 *         their respective properties in `Model`.
	 */
	public function configureClass($class) {
		return array(
			'meta' => array('key' => '_id', 'locked' => false),
			'schema' => array(
				'_id' => array('type' => 'string'),
			)
		);
	}

	/**
	 * With no parameter, always returns `true`, since CouchDB only depends on HTTP. With a
	 * parameter, queries for a specific supported feature.
	 *
	 * @param string $feature Test for support for a specific feature, i.e. `"transactions"` or
	 *               `"arrays"`.
	 * @return boolean Returns `true` if the particular feature support is enabled, otherwise
	 *         `false`.
	 */
	public static function enabled($feature = null) {
		if (!$feature) {
			return true;
		}
		$features = array(
			'arrays' => false,
			'transactions' => false,
			'booleans' => true,
			'relationships' => false
		);
		return isset($features[$feature]) ? $features[$feature] : null;
	}
	
	/**
	 * Executes calculation-related queries, such as those required for `count`.
	 *
	 * @param string $type Only accepts `count`.
	 * @param mixed $query The query to be executed.
	 * @param array $options Optional arguments for the `read()` query that will be executed
	 *        to obtain the calculation result.
	 * @return integer Result of the calculation.
	 */
	public function calculation($type, $query, array $options = array()) {
		$query->calculate($type);

		switch ($type) {
			case 'count':
				return $this->read($query, $options)->count();
		}
	}
	
	public function conditions($conditions, $context) {
		$model = $context->model();
		$key = $model::key();
		$data = $context->data();
		if (!isset($conditions[$key]) && isset($data['file'])) {
			switch(true) {
				case (is_array($data['file']) && isset($data['file']['name'])) :
					$conditions['object'] = $data['file']['name'];
					break;
				case ($context->md5()) :
					$conditions['object'] = $context->md5();
					break;
				default :
					$conditions['object'] = md5($data['file']);
			}
		}
		if (isset($conditions[$key])) {
			$conditions['object'] = $conditions[$key];
			unset($conditions[$key]);
		}
		return $conditions;
	}
	

	/**
	 * Returns a newly-created `Document` object, bound to a model and populated with default data
	 * and options.
	 *
	 * @param string $model A fully-namespaced class name representing the model class to which the
	 *               `Document` object will be bound.
	 * @param array $data The default data with which the new `Document` should be populated.
	 * @param array $options Any additional options to pass to the `Document`'s constructor
	 * @return object Returns a new, un-saved `Document` object bound to the model class specified
	 *         in `$model`.
	 */
	public function item($model, array $data = array(), array $options = array()) {
		return parent::item($model, $this->_format($data, $model::key()), $options);
	}
	
	/**
	 * Limit for query.
	 *
	 * @param string $limit
	 * @param string $context
	 * @return array
	 */
	public function limit($limit, $context) {
		return ($limit) ? array('max-keys' => $limit) : null;
	}
	
	public function create($query, array $options = array()) {
		$defaults = array(
			'multipart' => true,
			'retry'     => 10, //retry upload on error
			'chunk_size' => static::$CHUNK_SIZE,
			'encryption' => null,
		);
		$options += $defaults;
		$params = $query->export($this, array('keys' => array('source', 'conditions')));
		$source = (!empty($params['source'])) ? array('source' => $params['source']) : array();
		$conditions = ($params['conditions'] ?: array()) + $source;
		$source = (isset($conditions['source'])) ? $conditions['source'] : null;
		if (!$pathConfig = $this->_path(__FUNCTION__, $conditions)) {
			return false;
		}
		$pathConfig += array(
			'body'   => '',
			'method' => 'PUT',
			);
		$data = $query->data();
		$multipart = $options['multipart'];
		switch (true) {
			case (array_key_exists('file', $data)):
				$uploadKeys = array('name', 'type', 'tmp_name', 'error', 'size');
				//if file is just a bytestream, set default parameters 
				$file = (is_array($data['file']) && array_keys($data['file']) == $uploadKeys) ? 
					$data['file'] : 
					array(
						'tmp_name' => $data['file'],
						'type'     => 'application/octet-stream',
					);
				unset($data['file']);
				$data += $file;
				$fileExist = file_exists($file['tmp_name']);
				$multipart = $multipart && $fileExist && $file['size'] > $options['chunk_size'];
				$pathConfig['body'] = ($fileExist && !$multipart) ? file_get_contents($file['tmp_name']) : $file['tmp_name'];
			break;
			case ($source):
				$pathConfig['type'] = 'application/xml';
				$multipart = false;
				if (array_key_exists('region', $data)) {
					$region = array_search($data['region'], $this->_regions);
					if ($region === false) {
						return false;
					}
					$xmlElem = simplexml_load_string("<CreateBucketConfiguration></CreateBucketConfiguration>");
					$xmlElem->addAttribute('xmlns', $this->_xmlns);
					$xmlElem->addChild("LocationConstraint", $this->_regions[$region]);
					$pathConfig['body'] = $xmlElem->asXml();
				}
			break;
			default :
				return false;
		}
		$pathConfig += $conditions + $data;
		$response = ($multipart) ? $this->_multipartUpload($query, $pathConfig, $options) : $this->_request($query, $pathConfig, $options);
		if (!$response) {
			return false;
		}
		$entity = $query->entity();
		if ($entity && isset($conditions['object'])) {
			$entity->sync($conditions['object']);		
		}
		return true;
	}
	
	public function delete($query, array $options = array()) {
		$defaults = array(
			'quiet'      => 'true',
		);
		$options += $defaults;
		$params = $query->export($this, array('keys' => array('source', 'conditions')));
		$source = $params['source'];
		$conditions = compact('source') + (array)$params['conditions'];
		$multiple = false;
		if (isset($conditions['object']) && is_array($conditions['object'])) {
			$multiple = true;
			$conditions += array('context' => array('delete'));
		}
		if (!$pathConfig = $this->_path(__FUNCTION__, $conditions)) {
			return null;
		}
		if ($multiple) {
			$keys = $conditions['object'];
			unset($conditions['object']);
			$method = 'POST';
			$pathConfig['type'] = 'application/xml';
			$maxKeysInRequest = 1000;
			$maxKeys = count($keys);
			$buildObjects = function($xmlElem, $objects) {
				foreach ($objects as $object) {
					$xmlObj = $xmlElem->addChild("Object");
					if (is_array($object)) {
						if (isset($object['object'])) {
							$xmlObj->addChild('Key', $object['object']);
						}
						if (isset($object['versionid'])) {
							$xmlObj->addChild('VersionId', $object['versionid']);
						}
					} else {
						$xmlObj->addChild('Key', $object);
					}
				}
			};
			$errorMessages = '';
			//build requests
			$requestBodys = array();
			$chunks = array_chunk($keys, $maxKeysInRequest);
			foreach ($chunks as $chunk) {
				$xmlElem = simplexml_load_string("<Delete></Delete>");
				if ($options['quiet'] == 'true') {
					$xmlElem->addChild("Quiet", $options['quiet']);
				}
				$buildObjects($xmlElem, $chunk);
				$requestBodys[] = $xmlElem->asXML();
			}
			$pathConfig['body'] = $requestBodys;
			$responses = $this->_multipleRequest($query, $conditions + $pathConfig + compact('method'));
			foreach ($responses as $response) {
				if ($response && strpos($response->headers['Content-Type'], 'xml')) {
					$xml = simplexml_load_string($response->body);
					$xml = $this->_filterResponse($xml);
					if (is_array($xml)) {
						$errorMessages .= array_reduce($xml, function($res, $val) {return $res .= "{$val->Key}: {$val->Message}\n";}, '');
					}
				} else {
					return false;
				}			
			}
			return empty($errorMessages);
		}
		$method = 'DELETE';
		if (!$response = $this->_request($query, $conditions + $pathConfig + compact('method'))) {
			return false;
		} 
		return $response->status['code'] == '204';
	}	
	
	public function read($query, array $options = array()) {
		$defaults = array(
			'type'   => 'read'
		);
		$options += $defaults;
		$return = $query->return() ?: 'response'; // return type
		$options += compact('return');
		$params = $query->export($this, array('keys' => array('source', 'conditions', 'limit')));
		$conditions = (array) $params['conditions'];
		$source = $params['source'];
		$conditions += ($source) ? compact('source') : array();
		$limit = $params['limit'];
		$exist = $options['type'] == 'exist';
		$model = $query->model();
		//include limit in context for path generation
		$context = (isset($conditions['context'])) ? $conditions['context'] : array();
		if (!isset($conditions['object']) && $source && $limit) {
			$context += array('context' => $context + $limit);
		}
		if (!$pathConfig = $this->_path(__FUNCTION__, $context + $conditions)) {
			return null;
		}
		$pathConfig += array(
			'body'   => '',
			'method' => ($exist) ? 'HEAD' : 'GET',
			);
		
		if (!$response = $this->_request($query, $conditions + $pathConfig, $options)) {
			$data = array();
		} else {
			if ($return != 'stream' && !$exist && strpos($response->headers['Content-Type'], 'xml')) {
				$xml = simplexml_load_string($response->body);
				$xml = $this->_filterResponse($xml);
				$data = $this->_simpleXml2Array($xml);
			} else {
				$entity = array(
					'headers' => $response->headers,
					'file' => $response->body, 
				);
				$entity += isset($conditions['object']) ? array('key' => $conditions['object']) : compact('source');
				$data = array($entity);
			}
		}
		foreach ($data as $key => $val) {
			$data[$key] = $this->item($model, $val, array('exists' => true));
		}
		return $this->item($query->model(), $data, array('class' => 'set'));
	}	

	public function sources($class = null) {
		return; //array_keys($this->_sources);
	}

}

?>
