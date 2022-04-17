<?php

namespace Kafka;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;

class KafkaQueue extends Queue implements QueueContract
{
    protected $consumer;
    protected $producer;

    public function __construct($producer, $consumer)
    {
        $this->producer = $producer;
        $this->consumer = $consumer;
    }

    public function size($queue = null)
    {
    }

    public function push($job, $data = '', $queue = null)
    {
        $topic = $this->producer->newTopic($queue ?? env('KAFKA_QUEUE'));
        // $topic->produce(RD_KAFKA_PARTITION_UA, 0, "hello from the other app");
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        $this->producer->flush(1000);
    }

    public function pushOn($queue, $job, $data = '')
    {
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
    }

    public function laterOn($queue, $delay, $job, $data = '')
    {
    }

    public function bulk($jobs, $data = '', $queue = null)
    {
    }

    public function pop($queue = null)
    {
        $this->consumer->subscribe([$queue]);
        $message = $this->consumer->consume(120 * 1000);

        try {
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    var_dump("RD_KAFKA_RESP_ERR_NO_ERROR");
                    $job = unserialize($message->payload);
                    $job->handle();
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    var_dump("- No more messages; will wait for more -");
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    var_dump("- Timed out -");
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        } catch (\Exception $e) {
            var_dump($e->getMessage());
        }
    }

    // public function getConnectionName()
    // {
    // }

    // public function setConnectionName($name)
    // {
    // }
}
