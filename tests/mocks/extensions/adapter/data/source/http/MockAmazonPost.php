<?php

namespace li3_aws\tests\mocks\extensions\adapter\data\source\http;

use lithium\data\DocumentSchema;

class MockAmazonPost extends \lithium\tests\mocks\data\MockBase {

	public static $connection;

	protected $_meta = array(
		'source' => 'foo-bucket', 
		'connection' => false, 
		'key' => '_id',
		);

	protected $_schema = array(
		'_id' => array('type' => 'string'),
	);

	public static function resetSchema($array = false) {
		if ($array) {
			return static::_object()->_schema = array();
		}
		static::_object()->_schema = new DocumentSchema();
	}
}

?>