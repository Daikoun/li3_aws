<?php

namespace li3_aws\tests\cases\extensions\adapter\data\source\http;

use lithium\data\model\Query;
use lithium\data\entity\Document;
use li3_aws\extensions\adapter\data\source\http\AmazonS3;

class AmazonS3Test extends \lithium\test\Unit {

	public $db;

	protected $_testConfig = array(
		'persistent' => false,
		'scheme' => 'http',
		'host' => 'localhost',
		'key' => 'foo',
		'secret' => 'bar',
		'port' => 80,
		'timeout' => 30,
		'socket' => 'li3_aws\tests\mocks\extensions\adapter\data\source\http\MockAmazonSocket'
	);

	protected $_model = 'li3_aws\tests\mocks\extensions\adapter\data\source\http\MockAmazonPost';

	protected function _encrypt($string2sign) {
		$aws_secret = $this->_testConfig['secret'];
		//assuming key is global $aws_secret 40 bytes long
		$aws_secret = (strlen($aws_secret) == 40) ? $aws_secret.str_repeat(chr(0), 24) : $aws_secret;
		$ipad = str_repeat(chr(0x36), 64);
		$opad = str_repeat(chr(0x5c), 64);
		$hmac = sha1(($aws_secret^$opad).sha1(($aws_secret^$ipad).$string2sign, true), true);
		$signature = base64_encode($hmac);
		return "AWS {$this->_testConfig['key']}:{$signature}";
	}
	
	public function setUp() {
		$this->db = new AmazonS3($this->_testConfig);
		$model = $this->_model;
		$model::$connection = $this->db;
		$model::resetSchema();
	}

	public function tearDown() {
		$socket = $this->_testConfig['socket'];
		$socket::resetData();
	}
	
	public function testAllMethodsNoConnection() {
		$this->db = new AmazonS3(array('socket' => false));
		$this->assertTrue($this->db->connect());
		$this->assertTrue($this->db->disconnect());
		$this->assertNull($this->db->get());
		$this->assertNull($this->db->post());
		$this->assertNull($this->db->put());
	}

	public function testConnect() {
		$result = $this->db->connect();
		$this->assertTrue($result);
	}

	public function testDisconnect() {
		$result = $this->db->connect();
		$this->assertTrue($result);

		$result = $this->db->disconnect();
		$this->assertTrue($result);
	}

	public function testSources() {
		$result = $this->db->sources();
		$this->assertNull($result);
	}

	public function testDescribe() {
		$this->assertEqual(array(), $this->db->describe('companies'));
	}

	public function testItem() {
		$data = array('key' => 'foo.txt', 'name' => 'bar_bucket', 'content' => 'foobarbaz');
		$expected = array('_id' => 'foo.txt', 'source' => 'bar_bucket', 'content' => 'foobarbaz');
		$item = $this->db->item(/*$this->query->model()*/ $this->_model, $data);
		$result = $item->data();
		$this->assertEqual($expected, $result);
	}

	public function testCreate() {
		//create bucket
		$model = $this->_model;
		$socket = $this->_testConfig['socket'];
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 200 OK',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Location: /foo-bucket1',
			'Content-Length: 0',
			'Connection: close',
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$bucket = 'foo-bucket1';
		$model::meta('source', $bucket);
//		$entity = new Document(compact('model'));
//		$this->query = new Query(compact('model', 'entity'));
//		$result = $this->db->create($this->query);
//		$this->assertTrue($result);
//		$request = $this->db->last->request;
//		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
//		$this->assertEqual("/", $request->path);
//		$this->assertEqual('application/xml', $request->headers['Content-Type']);
//		$this->assertEqual(0, $request->headers['Content-Length']);
//		$this->assertNotEqual("", $request->headers['Date']);
//		$date = $request->headers['Date'];
//		$this->assertEqual('PUT', $request->method);
//		$this->assertEqual($this->_encrypt("PUT\n\napplication/xml\n{$date}\n/{$bucket}/"), $request->headers['Authorization']);
//		$this->assertEqual('', $request->body);
//		$this->assertNull($entity->_id);
		//create file without specifying an id but setting file-size
		$model::meta('source', $bucket);
		$entity = new Document(compact('model'));
		$this->query = new Query(compact('model', 'entity') + array('type' => 'create'));
		$text = "Testtext";
		$this->query->data(array(
			'file' => array(
				'name'     => 'test.txt',
				'type'     => 'plain/text',
				'tmp_name' => $text,
				'error'    => 0,
				'size'     => strlen($text),
			),
		));
		$result = $this->db->create($this->query);
		$this->assertTrue($result);
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/test.txt", $request->path);
		$this->assertEqual('plain/text', $request->headers['Content-Type']);
		$this->assertEqual(strlen($text), $request->headers['Content-Length']);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('PUT', $request->method);
		$md5 = base64_encode(md5($text, true));
		$this->assertEqual($this->_encrypt("PUT\n{$md5}\nplain/text\n{$date}\n/{$bucket}/test.txt"), $request->headers['Authorization']);
		$this->assertEqual($text, $request->body);
		$this->assertEqual('test.txt', $entity->_id);
		//create file by specifying an id and set explicitly the md5 and avoid the media handler
		$entity = new Document(compact('model'));
		$this->query = new Query(compact('model', 'entity') + array('type' => 'create'));
		$text = "Testmov.";
		$this->query->data(array(
			'_id' => 'foobar.mov',
			'file' => array(
				'name'     => 'test.avi',
				'type'     => 'application/foo',
				'tmp_name' => $text,
				'error'    => 0,
				'size'     => strlen($text),
			),
		));
		$md5 = md5('foo');
		$result = $this->db->create($this->query, compact('md5'));
		$this->assertTrue($result);
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/foobar.mov", $request->path);
		$this->assertEqual('application/foo', $request->headers['Content-Type']);
		$this->assertEqual(strlen($text), $request->headers['Content-Length']);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('PUT', $request->method);
		$md5 = base64_encode(md5('foo', true));
		$this->assertEqual($this->_encrypt("PUT\n{$md5}\napplication/foo\n{$date}\n/{$bucket}/foobar.mov"), $request->headers['Authorization']);
		$this->assertEqual($text, $request->body);
		$this->assertEqual('foobar.mov', $entity->_id);
        //create file by filename
		$text = "tempfile.";
		$name = 'tmp_file.txt';
		$tmp_name = tempnam(null, "tmp");
		$handle = fopen($tmp_name, "w");
		fwrite($handle, $text);
		fclose($handle);
		$finfo = finfo_open(FILEINFO_MIME_TYPE);
		$type = finfo_file($finfo, $tmp_name);
		$size = filesize($tmp_name);
		$error = 0;
		$entity = new Document(compact('model'));
		$this->query = new Query(compact('model', 'entity') + array('type' => 'create'));
		$this->query->data(array(
			'file' => compact('name', 'type', 'tmp_name', 'error', 'size'),
		));
		$result = $this->db->create($this->query);
		$this->assertTrue($result);
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/{$name}", $request->path);
		$this->assertEqual($type, $request->headers['Content-Type']);
		$this->assertEqual(strlen($text), $request->headers['Content-Length']);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('PUT', $request->method);
		$md5 = base64_encode(md5($text, true));
		$this->assertEqual($this->_encrypt("PUT\n{$md5}\n{$type}\n{$date}\n/{$bucket}/{$name}"), $request->headers['Authorization']);
		$this->assertEqual($text, $request->body);
		$this->assertEqual($name, $entity->_id);
        unlink($tmp_name);
		
		//upload just file contents and set encryption to AES256
		$entity = new Document(compact('model'));
		$this->query = new Query(compact('model', 'entity') + array('type' => 'create'));
		$text = "Testfile contents.";
		$this->query->data(array(
			'file' => $text,
		));
		$fileName = md5($text);
		$result = $this->db->create($this->query, array('encryption' => 'AES256'));
		$this->assertTrue($result);
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/{$fileName}", $request->path);
		$this->assertEqual('AES256', $request->headers['x-amz-server-side-encryption']);
		$this->assertEqual('application/octet-stream', $request->headers['Content-Type']);
		$this->assertEqual(strlen($text), $request->headers['Content-Length']);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('PUT', $request->method);
		$md5 = base64_encode(md5($text, true));
		$this->assertEqual($this->_encrypt("PUT\n{$md5}\napplication/octet-stream\n{$date}\nx-amz-server-side-encryption:AES256\n/{$bucket}/{$fileName}"), $request->headers['Authorization']);
		$this->assertEqual($text, $request->body);
		$this->assertEqual($fileName, $entity->_id);
		//upload file but bucket do not exist
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 404 Not Found',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Content-Length: 0',
			'Content-Type: application/xml',
			'Connection: close',
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$socket::$data .= '<?xml version="1.0" encoding="UTF-8"?>
			<Error>
			  <Code>NoSuchBucket</Code>
			  <Message>The specified bucket does not exist.</Message>
			  <Resource>/foo-bucket1/foobar.mov</Resource> 
              <RequestId>foobar</RequestId>
			</Error>';
		$entity = new Document(compact('model'));
		$this->query = new Query(compact('model', 'entity') + array('type' => 'create'));
		$text = "Testmov.";
		$this->query->data(array(
			'_id' => 'foobar.mov',
			'file' => array(
				'name'     => 'test.avi',
				'type'     => 'application/foo',
				'tmp_name' => $text,
				'error'    => 0,
				'size'     => strlen($text),
			),
		));
		$result = $this->db->create($this->query);
		$this->assertFalse($result);
		$this->assertEqual('The specified bucket does not exist.', $entity->errors('foo-bucket1'));
	}

	public function testCreateMultipartUpload() {
		$model = $this->_model;
		$socket = $this->_testConfig['socket'];
		$bucket = 'foo-bucket1';
		$model::meta('source', $bucket);
        //create file by filename
		$text = str_repeat('a', 1048576);
		$name = 'tmp_file.txt';
		$tmp_name = tempnam(null, "tmp");
		$handle = fopen($tmp_name, "w");
		fwrite($handle, $text); //create 1MB textfile
		fclose($handle);
		$finfo = finfo_open(FILEINFO_MIME_TYPE);
		$type = finfo_file($finfo, $tmp_name);
		$size = filesize($tmp_name);
		$error = 0;
		$entity = new Document(compact('model'));
		$this->query = new Query(compact('model', 'entity') + array('type' => 'create'));
		$this->query->data(array(
			'file' => compact('name', 'type', 'tmp_name', 'error', 'size'),
		));
		$result = $this->db->create($this->query, array('chunk_size' => 716800)); //chunk size 700kB
		$this->assertTrue($result);
		$chunks = $socket::$temp;
		$this->assertEqual(4, count($socket::$requests));
		$this->assertEqual('foo', $chunks['UploadId']);
		$this->assertEqual($name, $chunks['Key']);
		$this->assertEqual($bucket, $chunks['Bucket']);
		$this->assertEqual(2, count($chunks['data']));
		$this->assertEqual(716800, strlen($chunks['data'][1]));
		$this->assertEqual(1048576-716800, strlen($chunks['data'][2]));
		//test init multipart
		$request = $socket::$requests[0];
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/{$name}?uploads", $request->path);
		$this->assertEqual($type, $request->headers['Content-Type']);
		$this->assertEqual(0, $request->headers['Content-Length']);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('POST', $request->method);
		$md5 = base64_encode(md5($text, true));
		$this->assertEqual($this->_encrypt("POST\n\n{$type}\n{$date}\n/{$bucket}/{$name}?uploads"), $request->headers['Authorization']);
		$this->assertEqual('', $request->body);
		//test first part upload
		$request = $socket::$requests[1];
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/{$name}?uploadId={$chunks['UploadId']}&partNumber=1", $request->path);
		$this->assertEqual('application/octet-stream', $request->headers['Content-Type']);
		$this->assertEqual(716800, $request->headers['Content-Length']);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('PUT', $request->method);
		$md5 = base64_encode(md5(str_repeat('a', 716800), true));
		$this->assertEqual($this->_encrypt("PUT\n{$md5}\napplication/octet-stream\n{$date}\n/{$bucket}/{$name}?partNumber=1&uploadId={$chunks['UploadId']}"), $request->headers['Authorization']);
		$this->assertEqual(str_repeat('a', 716800), $request->body);
		$request = $socket::$requests[2];
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/{$name}?uploadId={$chunks['UploadId']}&partNumber=2", $request->path);
		$this->assertEqual('application/octet-stream', $request->headers['Content-Type']);
		$this->assertEqual(1048576-716800, $request->headers['Content-Length']);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('PUT', $request->method);
		$md5 = base64_encode(md5(str_repeat('a', 1048576-716800), true));
		$this->assertEqual($this->_encrypt("PUT\n{$md5}\napplication/octet-stream\n{$date}\n/{$bucket}/{$name}?partNumber=2&uploadId={$chunks['UploadId']}"), $request->headers['Authorization']);
		$this->assertEqual(str_repeat('a', 1048576-716800), $request->body);
		//test last request
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/{$name}?uploadId={$chunks['UploadId']}", $request->path);
		$this->assertEqual('application/xml', $request->headers['Content-Type']);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('POST', $request->method);
		$text = simplexml_load_string('<CompleteMultipartUpload></CompleteMultipartUpload>');
		$part = $text->addChild('Part');
		$part->addChild('PartNumber', 1);
		$part->addChild('ETag', 'foo1');
		$part = $text->addChild('Part');
		$part->addChild('PartNumber', 2);
		$part->addChild('ETag', 'foo2');
		$text = $text->asXML();
		$md5 = base64_encode(md5($text, true));
		$this->assertEqual($this->_encrypt("POST\n{$md5}\napplication/xml\n{$date}\n/{$bucket}/{$name}?uploadId={$chunks['UploadId']}"), $request->headers['Authorization']);
		$this->assertEqual($text, $request->body);
		$this->assertEqual($name, $entity->_id);
		
		$socket::resetData();
		
		//test multipart upload with encryption is set to AES256
		$entity = new Document(compact('model'));
		$this->query = new Query(compact('model', 'entity') + array('type' => 'create'));
		$this->query->data(array(
			'file' => compact('name', 'type', 'tmp_name', 'error', 'size'),
		));
		$result = $this->db->create($this->query, array('chunk_size' => 716800, 'encryption' => 'AES256')); //chunk size 700kB
		$this->assertTrue($result);
		$chunks = $socket::$temp;
		$this->assertEqual(4, count($socket::$requests));
		$this->assertEqual('foo', $chunks['UploadId']);
		$this->assertEqual($name, $chunks['Key']);
		$this->assertEqual($bucket, $chunks['Bucket']);
		$this->assertEqual(2, count($chunks['data']));
		$this->assertEqual(716800, strlen($chunks['data'][1]));
		$this->assertEqual(1048576-716800, strlen($chunks['data'][2]));
		//test init multipart
		$request = $socket::$requests[0];
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/{$name}?uploads", $request->path);
		$this->assertEqual($type, $request->headers['Content-Type']);
		$this->assertEqual(0, $request->headers['Content-Length']);
		$this->assertEqual('AES256', $request->headers['x-amz-server-side-encryption']);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('POST', $request->method);
		$md5 = base64_encode(md5($text, true));
		$this->assertEqual($this->_encrypt("POST\n\n{$type}\n{$date}\nx-amz-server-side-encryption:AES256\n/{$bucket}/{$name}?uploads"), $request->headers['Authorization']);
		$this->assertEqual('', $request->body);
		//test first part upload
		$request = $socket::$requests[1];
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/{$name}?uploadId={$chunks['UploadId']}&partNumber=1", $request->path);
		$this->assertEqual('application/octet-stream', $request->headers['Content-Type']);
		$this->assertEqual(716800, $request->headers['Content-Length']);
		$this->assertFalse(isset($request->headers['x-amz-server-side-encryption']));
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('PUT', $request->method);
		$md5 = base64_encode(md5(str_repeat('a', 716800), true));
		$this->assertEqual($this->_encrypt("PUT\n{$md5}\napplication/octet-stream\n{$date}\n/{$bucket}/{$name}?partNumber=1&uploadId={$chunks['UploadId']}"), $request->headers['Authorization']);
		$this->assertEqual(str_repeat('a', 716800), $request->body);
		$request = $socket::$requests[2];
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/{$name}?uploadId={$chunks['UploadId']}&partNumber=2", $request->path);
		$this->assertEqual('application/octet-stream', $request->headers['Content-Type']);
		$this->assertEqual(1048576-716800, $request->headers['Content-Length']);
		$this->assertFalse(isset($request->headers['x-amz-server-side-encryption']));
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('PUT', $request->method);
		$md5 = base64_encode(md5(str_repeat('a', 1048576-716800), true));
		$this->assertEqual($this->_encrypt("PUT\n{$md5}\napplication/octet-stream\n{$date}\n/{$bucket}/{$name}?partNumber=2&uploadId={$chunks['UploadId']}"), $request->headers['Authorization']);
		$this->assertEqual(str_repeat('a', 1048576-716800), $request->body);
		//test last request
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/{$name}?uploadId={$chunks['UploadId']}", $request->path);
		$this->assertEqual('application/xml', $request->headers['Content-Type']);
		$this->assertFalse(isset($request->headers['x-amz-server-side-encryption']));
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('POST', $request->method);
		$text = simplexml_load_string('<CompleteMultipartUpload></CompleteMultipartUpload>');
		$part = $text->addChild('Part');
		$part->addChild('PartNumber', 1);
		$part->addChild('ETag', 'foo1');
		$part = $text->addChild('Part');
		$part->addChild('PartNumber', 2);
		$part->addChild('ETag', 'foo2');
		$text = $text->asXML();
		$md5 = base64_encode(md5($text, true));
		$this->assertEqual($this->_encrypt("POST\n{$md5}\napplication/xml\n{$date}\n/{$bucket}/{$name}?uploadId={$chunks['UploadId']}"), $request->headers['Authorization']);
		$this->assertEqual($text, $request->body);
		$this->assertEqual($name, $entity->_id);

		unlink($tmp_name);
		
	}
	
	public function testCreateWithErrorAndNoEntity() {
		$model = $this->_model;
		$socket = $this->_testConfig['socket'];
		//upload file but bucket do not exist
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 404 Not Found',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Content-Length: 0',
			'Connection: close',
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$socket::$data .= '<?xml version="1.0" encoding="UTF-8"?>
			<Error>
			  <Code>NoSuchBucket</Code>
			  <Message>The specified bucket does not exist.</Message>
			  <Resource>/foo-bucket1/foobar.mov</Resource> 
              <RequestId>foobar</RequestId>
			</Error>';
		$this->query = new Query(compact('model') + array('type' => 'create'));
		$text = "Testmov.";
		$this->query->data(array(
			'_id' => 'foobar.mov',
			'file' => array(
				'name'     => 'test.avi',
				'type'     => 'application/foo',
				'tmp_name' => $text,
				'error'    => 0,
				'size'     => strlen($text),
			),
		));
		$result = $this->db->create($this->query);
		$this->assertFalse($result);
	}

	public function testRead() {
		$model = $this->_model;
		$socket = $this->_testConfig['socket'];
		$bucket = 'foo-bucket1';
		$model::meta('source', $bucket);
		//read bucket list
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 200 OK',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Content-Type: application/xml',
			'Content-Length: 0',
			'Connection: close',
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$socket::$data .= '<?xml version="1.0" encoding="UTF-8"?>
			<ListAllMyBucketsResult xmlns="http://doc.s3.amazonaws.com/2006-03-01">
				<Owner>
					<ID>bcaf1ffd86f461ca5fb16fd081034f</ID>
					<DisplayName>webfile</DisplayName>
				</Owner>
				<Buckets>
					<Bucket>
						<Name>quotes</Name>
						<CreationDate>2006-02-03T16:45:09.000Z</CreationDate>
					</Bucket>
					<Bucket>
						<Name>samples</Name>
						<CreationDate>2006-02-03T16:41:58.000Z</CreationDate>
					</Bucket>
				</Buckets>
			</ListAllMyBucketsResult>';
		$this->query = new Query(compact('model'));
		$this->query->source('');
		$result = $this->db->read($this->query);
		$request = $this->db->last->request;
		$this->assertEqual("s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/", $request->path);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('GET', $request->method);
		$this->assertEqual($this->_encrypt("GET\n\n\n{$date}\n/"), $request->headers['Authorization']);
		$this->assertIdentical('', $request->body);
		$this->assertEqual(2, count($result));
		$this->assertEqual(array('source' => 'quotes', 'creationdate' => '2006-02-03T16:45:09.000Z'), $result[0]->data());
		$this->assertEqual(array('source' => 'samples', 'creationdate' => '2006-02-03T16:41:58.000Z'), $result[1]->data());
		//test list buckets and ignore limit
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 200 OK',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Content-Type: application/xml',
			'Content-Length: 0',
			'Connection: close',
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$socket::$data .= '<?xml version="1.0" encoding="UTF-8"?>
			<ListAllMyBucketsResult xmlns="http://doc.s3.amazonaws.com/2006-03-01">
				<Owner>
					<ID>bcaf1ffd86f461ca5fb16fd081034f</ID>
					<DisplayName>webfile</DisplayName>
				</Owner>
				<Buckets>
					<Bucket>
						<Name>quotes</Name>
						<CreationDate>2006-02-03T16:45:09.000Z</CreationDate>
					</Bucket>
					<Bucket>
						<Name>samples</Name>
						<CreationDate>2006-02-03T16:41:58.000Z</CreationDate>
					</Bucket>
				</Buckets>
			</ListAllMyBucketsResult>';
		$this->query = new Query(compact('model'));
		$this->query->source('');
		$this->query->limit(1);
		$result = $this->db->read($this->query);
		$request = $this->db->last->request;
		$this->assertEqual("s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/", $request->path);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('GET', $request->method);
		$this->assertEqual($this->_encrypt("GET\n\n\n{$date}\n/"), $request->headers['Authorization']);
		$this->assertIdentical('', $request->body);
		$this->assertEqual(2, count($result));
		$this->assertEqual(array('source' => 'quotes', 'creationdate' => '2006-02-03T16:45:09.000Z'), $result[0]->data());
		$this->assertEqual(array('source' => 'samples', 'creationdate' => '2006-02-03T16:41:58.000Z'), $result[1]->data());
		//list objects in bucket
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 200 OK',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Content-Type: aplication/xml',
			'Content-Length: 0',
			'Connection: close',
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$socket::$data .= '<?xml version="1.0" encoding="UTF-8"?>
			<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
				<Name>bucket</Name>
				<Prefix/>
				<Marker/>
				<MaxKeys>1000</MaxKeys>
				<IsTruncated>false</IsTruncated>
				<Contents>
					<Key>my-image.jpg</Key>
					<LastModified>2009-10-12T17:50:30.000Z</LastModified>
					<ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
					<Size>434234</Size>
					<StorageClass>STANDARD</StorageClass>
					<Owner>
						<ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
						<DisplayName>mtd@amazon.com</DisplayName>
					</Owner>
				</Contents>
				<Contents>
					<Key>my-third-image.jpg</Key>
					<LastModified>2009-10-12T17:50:30.000Z</LastModified>
					<ETag>&quot;1b2cf535f27731c974343645a3985328&quot;</ETag>
					<Size>64994</Size>
					<StorageClass>STANDARD</StorageClass>
					<Owner>
						<ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
						<DisplayName>mtd@amazon.com</DisplayName>
					</Owner>
				</Contents>
			</ListBucketResult>';
		$this->query = new Query(compact('model'));
		$result = $this->db->read($this->query);
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/", $request->path);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('GET', $request->method);
		$this->assertEqual($this->_encrypt("GET\n\n\n{$date}\n/{$bucket}/"), $request->headers['Authorization']);
		$this->assertIdentical('', $request->body);
		$this->assertEqual(2, count($result));
		$expected = array(
			'_id' => 'my-image.jpg',
			'lastmodified' => '2009-10-12T17:50:30.000Z',
			'etag' => '"fba9dede5f27731c9771645a39863328"',
			'size' => '434234',
			'storageclass' => 'STANDARD',
			'owner' => array(
				'id' => '75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a',
				'displayname' => 'mtd@amazon.com',
			),
		);
		$this->assertEqual($expected, $result->first()->data());
		$expected = array(
			'_id' => 'my-third-image.jpg',
			'lastmodified' => '2009-10-12T17:50:30.000Z',
			'etag' => '"1b2cf535f27731c974343645a3985328"',
			'size' => '64994',
			'storageclass' => 'STANDARD',
			'owner' => array(
				'id' => '75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a',
				'displayname' => 'mtd@amazon.com',
			),
		);
		$this->assertEqual($expected, $result->next()->data());
		//test list objects and handle limit
		$this->query = new Query(compact('model'));
		$this->query->limit(1);
		$result = $this->db->read($this->query);
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/?max-keys=1", $request->path);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('GET', $request->method);
		$this->assertEqual($this->_encrypt("GET\n\n\n{$date}\n/{$bucket}/"), $request->headers['Authorization']);
		$this->assertIdentical('', $request->body);
		//test bucket exist
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 200 OK',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$this->query = new Query(compact('model'));
		$result = $this->db->read($this->query, array('type' => 'exist'));
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/", $request->path);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('HEAD', $request->method);
		$this->assertEqual($this->_encrypt("HEAD\n\n\n{$date}\n/{$bucket}/"), $request->headers['Authorization']);
		$this->assertIdentical('', $request->body);
		$this->assertEqual(1, count($result));
		//read object
		$text = 'testobject!';
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 200 OK',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Content-Type: text/plain',
			'Content-Length: '.  strlen($text),
			'Connection: close',
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$socket::$data .= $text;
		$this->query = new Query(compact('model'));
		$this->query->conditions(array('_id' => 'foo.txt'));
		$result = $this->db->read($this->query);
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/foo.txt", $request->path);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('GET', $request->method);
		$this->assertEqual($this->_encrypt("GET\n\n\n{$date}\n/{$bucket}/foo.txt"), $request->headers['Authorization']);
		$this->assertIdentical('', $request->body);
		$this->assertEqual(1, count($result));
		$result = $result->first();
		$this->assertEqual('text/plain', $result->headers['Content-Type']);
		$this->assertEqual(strlen($text), $result->headers['Content-Length']);
		$this->assertEqual('foo.txt', $result->_id);
		$this->assertEqual($text, $result->file);
		//read object and ignore limit
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 200 OK',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Content-Type: text/plain',
			'Content-Length: '.  strlen($text),
			'Connection: close',
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$socket::$data .= $text;
		$this->query = new Query(compact('model'));
		$this->query->conditions(array('_id' => 'foo.txt'));
		$this->query->limit(1);
		$result = $this->db->read($this->query);
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/foo.txt", $request->path);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('GET', $request->method);
		$this->assertEqual($this->_encrypt("GET\n\n\n{$date}\n/{$bucket}/foo.txt"), $request->headers['Authorization']);
		$this->assertIdentical('', $request->body);
		$this->assertEqual(1, count($result));
		//test object exist
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 200 OK',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Content-Length: 434234',
			'Content-Type: text/plain',
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$this->query = new Query(compact('model'));
		$this->query->conditions(array('_id' => 'foo.txt'));
		$result = $this->db->read($this->query, array('type' => 'exist'));
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/foo.txt", $request->path);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('HEAD', $request->method);
		$this->assertEqual($this->_encrypt("HEAD\n\n\n{$date}\n/{$bucket}/foo.txt"), $request->headers['Authorization']);
		$this->assertIdentical('', $request->body);
		$this->assertEqual(1, count($result));
		//read object stream
		$text = 'testobject!';
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 200 OK',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Content-Type: text/plain',
			'Content-Length: '.  strlen($text),
			'Connection: close',
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$socket::$data .= $text;
		$this->query = new Query(compact('model'));
		$this->query->conditions(array('_id' => 'foo.txt'));
		$this->query->return('stream');
		$result = $this->db->read($this->query);
		$this->assertEqual(1, count($result));
		$result = $result->first();
		$this->assertEqual('foo.txt', $result->_id);
		$content = $result->file->getBytes();
		$content = explode("\r\n\r\n", $content);
		$this->assertEqual($text, $content[1]);
		//test read nonexisting object
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 404 Not Found',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Content-Length: 0',
			'Content-Type: application/xml',
			'Connection: close',
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$socket::$data .= '<?xml version="1.0" encoding="UTF-8"?>
			<Error>
			  <Code>NoSuchKey</Code>
			  <Message>The specified key does not exist.</Message>
			  <Resource>/foo-bucket1/foo.txt</Resource> 
              <RequestId>foobar</RequestId>
			</Error>';
		$this->query = new Query(compact('model'));
		$this->query->conditions(array('_id' => 'foo.txt'));
		$result = $this->db->read($this->query);
		$this->assertEqual(0, count($result));
	}
	
/*	public function testUpdate() {
		$couchdb = new CouchDb($this->_testConfig);
		$this->query->data(array('id' => 12345, 'rev' => '1-1', 'title' => 'One'));

		$result = $couchdb->update($this->query);
		$this->assertTrue($result);

		$expected = '/lithium-test/12345';
		$result = $couchdb->last->request->path;
		$this->assertEqual($expected, $result);

		$expected = array();
		$result = $couchdb->last->request->query;
		$this->assertEqual($expected, $result);
	}
*/
	public function testDelete() {
		$model = $this->_model;
		$socket = $this->_testConfig['socket'];
		$bucket = 'foo-bucket1';
		$model::meta('source', $bucket);
		//delete bucket
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 204 No Content',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Connection: close',
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$this->query = new Query(compact('model'));
		$result = $this->db->delete($this->query);
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/", $request->path);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('DELETE', $request->method);
		$this->assertEqual($this->_encrypt("DELETE\n\n\n{$date}\n/{$bucket}/"), $request->headers['Authorization']);
		$this->assertIdentical('', $request->body);
		$this->assertTrue($result);
		//delete object
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 204 No Content',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Connection: close',
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$entity = new Document(compact('model'));
		$entity->_id = 'foo.txt';
		$this->query = new Query(compact('model', 'entity'));
		$result = $this->db->delete($this->query);
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/foo.txt", $request->path);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('DELETE', $request->method);
		$this->assertEqual($this->_encrypt("DELETE\n\n\n{$date}\n/{$bucket}/foo.txt"), $request->headers['Authorization']);
		$this->assertIdentical('', $request->body);
		$this->assertTrue($result);
		//delete object with error message
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 404 Not Found',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Connection: close',
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$entity = new Document(compact('model'));
		$entity->_id = 'foo.txt';
		$this->query = new Query(compact('model', 'entity'));
		$result = $this->db->delete($this->query);
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/foo.txt", $request->path);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('DELETE', $request->method);
		$this->assertEqual($this->_encrypt("DELETE\n\n\n{$date}\n/{$bucket}/foo.txt"), $request->headers['Authorization']);
		$this->assertIdentical('', $request->body);
		$this->assertFalse($result);
		//delete multiple objects
		$responseBody = '<?xml version="1.0" encoding="UTF-8"?>
			<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			</DeleteResult>';
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 200 OK',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Content-Type: application/xml',
			'Content-Length: '.strlen($responseBody),
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$socket::$data .= $responseBody; 
		$this->query = new Query(compact('model'));
		$this->query->conditions(array('_id' => array('foo.txt', 'bar.mov', 'baz.jpg')));
		$result = $this->db->delete($this->query);
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/?delete", $request->path);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('POST', $request->method);
		$body = '<?xml version="1.0"?><Delete><Quiet>true</Quiet>';
		$body .= '<Object><Key>foo.txt</Key></Object>';
		$body .= '<Object><Key>bar.mov</Key></Object>';
		$body .= '<Object><Key>baz.jpg</Key></Object></Delete>';
		$body = simplexml_load_string($body);
		$body = $body->asXML();
		$this->assertEqual(strlen($body), $request->headers['Content-Length']);
		$this->assertEqual($body, $request->body);
		$md5 = base64_encode(md5($body, true));
		$this->assertEqual($md5, $request->headers['Content-MD5']);
		$this->assertEqual($this->_encrypt("POST\n{$md5}\napplication/xml\n{$date}\n/{$bucket}/?delete"), $request->headers['Authorization']);
		$this->assertTrue($result);
		//delete more than 1000 objects
		$responseBody = '<?xml version="1.0" encoding="UTF-8"?>
			<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			</DeleteResult>';
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 200 OK',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Content-Type: application/xml',
			'Content-Length: '.strlen($responseBody),
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$socket::$data .= $responseBody; 
		$this->query = new Query(compact('model'));
		$ids = array();
		for ($i=0; $i<1001; $i++) {
			$ids[] = "foo_{$i}.txt";
		}
		$this->query->conditions(array('_id' => $ids));
		$result = $this->db->delete($this->query);
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/?delete", $request->path);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('POST', $request->method);
		$body = '<?xml version="1.0"?><Delete><Quiet>true</Quiet>';
		$body .= '<Object><Key>foo_1000.txt</Key></Object></Delete>';
		$body = simplexml_load_string($body);
		$body = $body->asXML();
		$this->assertEqual(strlen($body), $request->headers['Content-Length']);
		$this->assertEqual($body, $request->body);
		$md5 = base64_encode(md5($body, true));
		$this->assertEqual($md5, $request->headers['Content-MD5']);
		$this->assertEqual($this->_encrypt("POST\n{$md5}\napplication/xml\n{$date}\n/{$bucket}/?delete"), $request->headers['Authorization']);
		$this->assertTrue($result);
		//delete more than 1000 objects with error message
		$responseBody = '<?xml version="1.0" encoding="UTF-8"?>
			<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
				<Error>
					<Key>sample2.txt</Key>
					<Code>AccessDenied</Code>
					<Message>Access Denied</Message>
				</Error>
				<Error>
					<Key>sample3.jpg</Key>
					<Code>AccessDenied</Code>
					<Message>Access Denied</Message>
				</Error>
			</DeleteResult>';
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 200 OK',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Content-Type: application/xml',
			'Content-Length: '.strlen($responseBody),
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$socket::$data .= $responseBody; 
		$this->query = new Query(compact('model'));
		$this->query->conditions(array('_id' => $ids));
		$result = $this->db->delete($this->query);
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/?delete", $request->path);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('POST', $request->method);
		$body = '<?xml version="1.0"?><Delete><Quiet>true</Quiet>';
		$body .= '<Object><Key>foo_1000.txt</Key></Object></Delete>';
		$body = simplexml_load_string($body);
		$body = $body->asXML();
		$this->assertEqual(strlen($body), $request->headers['Content-Length']);
		$this->assertEqual($body, $request->body);
		$md5 = base64_encode(md5($body, true));
		$this->assertEqual($md5, $request->headers['Content-MD5']);
		$this->assertEqual($this->_encrypt("POST\n{$md5}\napplication/xml\n{$date}\n/{$bucket}/?delete"), $request->headers['Authorization']);
		$this->assertFalse($result);
		//delete more than 1000 objects with bucket error message
		$responseBody = '<?xml version="1.0" encoding="UTF-8"?>
			<Error>
			  <Code>NoSuchBucket</Code>
			  <Message>The specified bucket does not exist.</Message>
			  <Resource>/foo-bucket1/foo.txt</Resource> 
              <RequestId>foobar</RequestId>
			</Error>';
		$socket::$data = join("\r\n", array(
			'HTTP/1.1 404 Not Found',
			'x-amz-id-2: foo',
			'x-amz-request-id: bar',
			'Date: Wed, 01 Mar  2009 12:00:00 GMT',
			'Content-Type: application/xml',
			'Content-Length: '.strlen($responseBody),
			'Server: AmazonS3',
			)) . "\r\n\r\n";
		$socket::$data .= $responseBody; 
		$this->query = new Query(compact('model'));
		$this->query->conditions(array('_id' => $ids));
		$result = $this->db->delete($this->query);
		$request = $this->db->last->request;
		$this->assertEqual("{$bucket}.s3.{$this->_testConfig['host']}", $request->host);
		$this->assertEqual("/?delete", $request->path);
		$this->assertNotEqual("", $request->headers['Date']);
		$date = $request->headers['Date'];
		$this->assertEqual('POST', $request->method);
		$body = '<?xml version="1.0"?><Delete><Quiet>true</Quiet>';
		$body .= '<Object><Key>foo_1000.txt</Key></Object></Delete>';
		$body = simplexml_load_string($body);
		$body = $body->asXML();
		$this->assertEqual(strlen($body), $request->headers['Content-Length']);
		$this->assertEqual($body, $request->body);
		$md5 = base64_encode(md5($body, true));
		$this->assertEqual($md5, $request->headers['Content-MD5']);
		$this->assertEqual($this->_encrypt("POST\n{$md5}\napplication/xml\n{$date}\n/{$bucket}/?delete"), $request->headers['Authorization']);
		$this->assertFalse($result);
	}
}

?>