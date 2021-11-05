package com.cqz.component.flink.sql.catalog;

import org.apache.flink.connector.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.flink.connector.jdbc.catalog.PostgresCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MysqlCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresCatalog.class);

    public static final String DEFAULT_DATABASE = "mysql";


    public MysqlCatalog(
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return null;
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        return null;
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return false;
    }
}
