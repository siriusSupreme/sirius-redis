<?php

namespace Sirius\Redis\Contracts;

interface Factory
{
    /**
     * Get a Redis connection by name.
     *
     * @param  string  $name
     *
     * @return \Sirius\Redis\Abstracts\Connection
     */
    public function connection($name = null);
}
