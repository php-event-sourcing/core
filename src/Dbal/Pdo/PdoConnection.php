<?php

declare(strict_types=1);

namespace DbalEs\Dbal\Pdo;

use DbalEs\Dbal\Connection;
use DbalEs\Dbal\Statement;

class PdoConnection implements Connection
{
    public function __construct(
        private \PDO $pdo
    ) {
    }


    public function prepare(string $query): Statement
    {
        return new PdoStatement($this->pdo->prepare($query));
    }
}