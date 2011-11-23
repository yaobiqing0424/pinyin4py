<?php
require_once dirname(__FILE__) . '/aps/aps-client.php';
$context = new ZMQContext();

$client = new APSClient($context, array($argv[1]));

$client->start_request('convert', array($argv[2], 'df', TRUE, TRUE),
    function($reply, $status) {
       echo "$status - $reply\n";
    }, 2000
);

$pending = APSClient::wait_for_replies(4);
