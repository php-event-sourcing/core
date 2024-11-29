<?php

declare(strict_types=1);

namespace DbalEs\Dbal;

interface Statement
{
    public function execute(array $params = []): void;
    public function fetch(): array|false;
    public function fetchColumn(int $columnNumber = 0): mixed;
}