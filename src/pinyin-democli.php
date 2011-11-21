<?php
require_once dirname(__FILE__) . '/aps/aps-client.php';
$context = new ZMQContext();

$client = new APSClient($context, array('tcp://192.168.201.109:50000'));

$client->start_request('convert', array('你好世界'),
    function($reply, $status) {
       echo "$status - $reply\n";
    }, 2000
);

$client->start_request('convert', array('总部好租技术团队', 'tn'),
    function($reply, $status) {
       echo "$status - $reply\n";
    }, 2000
);

$client->start_request('convert', array('总部 好租 技术 团队', 'fl', false),
    function($reply, $status) {
       echo "$status - $reply\n";
    }, 2000
);

echo "Wait for replies\n";
$pending = APSClient::wait_for_replies(4);
