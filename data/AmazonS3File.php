<?php

namespace li3_aws\data;

class AmazonS3File extends \lithium\core\Object {

	protected $_autoConfig = array('stream', 'filename', 'size');
	
	public function __construct(array $config = array()) {
		$defaults = array(
			'stream' => null,
			'filename' => '', 
			'size' => 0,
		);
		parent::__construct($config + $defaults);
	}

	public function getResource() {
		return $this->_stream;
	}
	
	public function getFilename() {
		return $this->_filename;
	}
	
	public function getBytes() {
		return ($this->_stream) ? stream_get_contents($this->_stream) : '';
	}
	
	public function getSize() {
		return $this->_size;
	}
	
	public function write($filename) {
		if ($this->_stream) {
			try {
				$dest = fopen($filename, 'w');
				stream_copy_to_stream($this->_stream, $dest);
				fclose($dest);
				fclose($this->_stream);
			} catch (Exception $e) {
				return false;
			} 
			return true;
		}
		return false;
	}
	
}

?>
