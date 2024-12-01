<?php

declare(strict_types=1);

namespace DbalEs\Dbal;

interface Connection
{
    public function prepare(string $query): Statement;
    public function beginTransaction(): bool;
    public function commit(): bool;
    public function rollBack(): void;
}