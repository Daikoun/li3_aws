li3_aws
=======

Lithium plugin for accessing Amazon Web Services.
Current services to use:
* Amazon S3

How to install?
---------------

Clone the plugin into the `libraries` folder of your lithium application.

```
git clone git://github.com/Daikoun/li3_aws.git my_lithium_app/libraries
```

Enable the plugin in `my_lithium_app/app/config/bootstrap/libraries.php`.

```php
Libraries::add('li3_aws');
```
Setup the connection to Amazon S3 in `my_lithium_app/app/config/bootstrap/connections.php`.

```php
Connections::add('amazon_s3', array(
	'type'    => 'http',
	'adapter' => 'AmazonS3',
	'key'	  => 'my_key',
	'secret'  => 'my_secret',
));
```
The key and secret you need to copy from your AWS account.

Go to the AWS Management Console -> S3 and create a new bucket.

Create a new Model in lithium and setup the bucket name in the meta array.

```php
<?php

namespace app\models;

class Pictures extends \lithium\data\Model {

	protected $_meta = array(
		'source' => 'myBucketName',
		'connection' => 'amazon_s3',
	);
	
	public $validates = array();
}

?>
```

And you are ready to go.

Create Buckets:
---------------

Use the AWS Management Console (preferred).

Delete Buckets:
---------------

Use the AWS Management Console (preferred).


Store files in your bucket:
---------------------------

In your controller, you can do file uploads the same way you do that in MongoDB-GridFs.

```php
	public function add() {
		$picture = Pictures::create();

		if (($this->request->data) && $picture->save($this->request->data)) {
			return $this->redirect(array('Pictures::view', 'args' => array($picture->_id)));
		}
		return compact('picture');
	}
```

Where the request can contain an uploaded file by using a HTML form.

Store files encrypted in S3.

```php
	public function add() {
		$picture = Pictures::create();

		if (($this->request->data) && $picture->save($this->request->data, array('encryption' => 'AES256'))) {
			return $this->redirect(array('Pictures::view', 'args' => array($picture->_id)));
		}
		return compact('picture');
	}
```

List files in your bucket:
---------------------------

Currently you can only list the first 1000 files in the bucket. (Will be updated soon)
You can use the already generated index function in your lithium controller, e.g.:

```php
	public function index() {
		$pictures = Pictures::all();
		return compact('pictures');
	}
```

Download files from your bucket:
--------------------------------

The structure is similar to use Mongo-GridFs, so that you can easily exchange the storage without changing any source code, just by set the connection in bootstrap.
For optimizing downloads from S3, an additional Streamwrapper-Method is implemented.

a) Without StreamWrapper:

Using this method, the file contents are buffered in the PHP memory before you can access them. This can cause memory exceptions, if file size exceeds the PHP memory and also slows down the download process because you have to wait till the whole file is buffered.

```php
	public function view() {
		$picture = Pictures::first($this->request->id);
		$this->_render['auto'] = false;
		$this->response->headers('Content-Type', $picture->headers['Content-Type']);
		$this->response->headers('Content-Length', $picture->headers['Content-Length']);
		$this->response->body = $picture->file;
		return $this->response;
	}
```

`$picture->file` contains the file-contents stored as plain text or binary image data.

b) With StreamWrapper:

By using the StreamWrapper you can avoid buffering the file contents. The StreamWrapper helps you to directly pipe the file contents to the output. Another benefit, you can soon provide the download response.

```php
	public function view() {
		$id = $this->request->id;
		$picture = Pictures::first(array('conditions' => array('_id' => $id), 'return' => 'stream'));
		$this->_render['auto'] = false;
		$this->response->headers('Content-Type', $picture->headers['Content-Type']);
		$this->response->headers('Content-Length', $picture->headers['Content-Length']);
		$this->response->body = $picture->file->getBytes();
		return $this->response;
	}
```
In this case `$picture->file` is the StreamWrapper-Class `li3_aws/data/AmazonS3File.php` and provides two functions:
* `getBytes()` gives you a string with the whole contents of the file. Actually the contents are also buffered in the PHP memory. 
* `getResource()` gives you a stream object to control the content stream by hand, e.g.:

```php
	public function view() {
		$id = $this->request->id;
		$picture = Pictures::first(array('conditions' => array('_id' => $id), 'return' => 'stream'));
		$this->_render['auto'] = false;
		header("Content-Type: {$picture->headers['Content-Type']}");
		header("Content-Length: {$picture->headers['Content-Length']}");
		$stream = $picture->file->getResource();
		while (!feof($stream)) {
			echo fgets($stream, 16384);
		} 
		fclose($stream);
		return;
	}
``` 

By using `getResource()` you can minimize buffering and pipe the contents to output soon.


Delete Files from your Bucket:
------------------------------

a) Use Entity: 

If you use entity, you need two requests to S3. By using the StreamWrapper you can optimize the first request a little bit.

```php
	public function delete() {
		if (!$this->request->is('post') && !$this->request->is('delete')) {
			$msg = "Pictures::delete can only be called with http:post or http:delete.";
			throw new DispatchException($msg);
		}
		$id = $this->request->id;		
		Pictures::first(array('conditions' => array('_id' => $id), 'return' => 'stream'))->delete();
		return $this->redirect('Pictures::index');
	}
```

b) Use Model: 

The preferred way is to use Model directly to delete the File where only one request is needed.

```php
	public function delete() {
		if (!$this->request->is('post') && !$this->request->is('delete')) {
			$msg = "Pictures::delete can only be called with http:post or http:delete.";
			throw new DispatchException($msg);
		}
		$id = $this->request->id;		
		Pictures::remove(array('_id' => $this->request->id));
		return $this->redirect('Pictures::index');
	}
```

Using the Model allows you to delete multiple files in one request.

```php
	Pictures::remove(array('_id' => array('foo.jpg', 'bar.jpg'));
```

Test Cases:
-----------

The `li3_aws` plugin provides you with a mock class simulating access to Amazon S3.
You can setup your environment, that you access local Mongo-GridFs on development, the AmazonS3-Mock on testing and real Amazon S3 on production.

```php
Connections::add('amazon_s3', array(
	'development' => array(
		'type'     => 'MongoDb',
		'host'     => 'localhost',
		'database' => 'picturefs',
		'source'   => 'fs.files',
    ), 
	'test' => array(
		'type'    => 'http',
		'adapter' => 'AmazonS3',
		'key'	  => 'foo',
		'secret'  => 'bar',
		'socket'  => 'li3_aws\tests\mocks\extensions\adapter\data\source\http\MockAmazonSocket'
	),
	'production' => array(
		'type'    => 'http',
		'adapter' => 'AmazonS3',
		'key'	  => 'my_key',
		'secret'  => 'my_secret',
    ),
));
```
