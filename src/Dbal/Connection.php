<?php

declare(strict_types=1);

namespace DbalEs\Dbal;

interface Connection
{
    public function prepare(string $query): Statement;
}