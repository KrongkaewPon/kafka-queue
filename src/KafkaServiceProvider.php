<?php

namespace Kafka;

use Illuminate\Support\ServiceProvider;
use App\Connector\KafkaConnector;

class KafkaServiceProvider extends ServiceProvider
{
    /**
     * Bootstrap services.
     *
     * @return void
     */
    public function boot()
    {
        $manager = $this->app['queue'];

        $manager->addConnector('kafka', function () {
            return new KafkaConnector;
        });
    }
}
