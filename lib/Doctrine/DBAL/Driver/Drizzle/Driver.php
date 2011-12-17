<?php
namespace Doctrine\DBAL\Driver\Drizzle;

/**
 * A Doctrine DBAL driver for the Drizzle Database
 *
 * @author Andreas Streichardt <andreas.streichardt@gmail.com>
 * @since 2.0
 */
class Driver implements \Doctrine\DBAL\Driver
{
    public function connect(array $params, $username = null, $password = null, array $driverOptions = array())
    {
        return new Connection(
            $params['host'],
            $params['port'],
            $params['dbname'],
            $username,
            $password,
        );
    }

    public function getDatabasePlatform()
    {
        return new \Doctrine\DBAL\Platforms\DrizzlePlatform();
    }

    public function getSchemaManager(\Doctrine\DBAL\Connection $conn)
    {
        return new \Doctrine\DBAL\Schema\DrizzleSchemaManager($conn);
    }

    public function getName()
    {
        return 'drizzle';
    }

    public function getDatabase(\Doctrine\DBAL\Connection $conn)
    {
        $params = $conn->getParams();

        if (isset($params['dbname'])) {
            return $params['dbname'];
        }
        return $conn->query('SELECT DATABASE()')->fetchColumn();
    }
}
