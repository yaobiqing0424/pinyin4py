<?php
require_once dirname(__FILE__) . '/aps/aps-client.php';
$context = new ZMQContext();

$client = new APSClient($context, array('tcp://127.0.0.1:50000'));

$client->start_request('convert', array('你好世界'),
    function($reply, $status) {
       echo "$status - $reply\n";
    }, 2000
);

echo "Wait for replies\n";
$pending = APSClient::wait_for_replies(4000);