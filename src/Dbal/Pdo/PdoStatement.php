<?php

declare(strict_types=1);

namespace DbalEs\Dbal\Pdo;

use DbalEs\Dbal\Statement;

class PdoStatement implements Statement
{
    public function __construct(
        private \PDOStatement $pdoStatement
    ) {
    }


    public function execute(array $params = []): void
    {
        $this->pdoStatement->execute($params);
    }

    public function fetch(): array|false
    {
        return $this->pdoStatement->fetch(\PDO::FETCH_ASSOC);
    }

    public function fetchColumn(int $columnNumber = 0): mixed
    {
        return $this->pdoStatement->fetchColumn($columnNumber);
    }

    public function rowCount(): int
    {
        return $this->pdoStatement->rowCount();
    }
}