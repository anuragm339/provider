package com.example.messaging.config;

import com.example.messaging.storage.db.rocks.config.RocksConfigFactory;
import com.example.messaging.storage.db.rocks.config.RocksProperties;
import com.example.messaging.storage.db.rocks.duplicate.BidirectionalDuplicateHandler;
import com.example.messaging.storage.db.sqlite.SQLiteConfig;
import com.example.messaging.storage.service.StorageConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;

@Factory
public class DatabaseFactory {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseFactory.class);

    @Singleton
    @Primary
    public DataSource dataSource(SQLiteConfig config) {
        // Ensure database directory exists
        createDatabaseDirectory(config.getDbPath());
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:sqlite:" + config.getDbPath());
        // SQLite-specific configurations
        hikariConfig.addDataSourceProperty("journal_mode", "WAL");
        hikariConfig.addDataSourceProperty("synchronous", "NORMAL");
        hikariConfig.addDataSourceProperty("foreign_keys", "ON");

        return new HikariDataSource(hikariConfig);
    }


    private void createDatabaseDirectory(String dbPath) {
        try {
            File dbFile = new File(dbPath);
            File dbDir = dbFile.getParentFile();
            if (dbDir != null && !dbDir.exists()) {
                boolean created = dbDir.mkdirs();
                if (created) {
                    logger.info("Created database directory: {}", dbDir.getAbsolutePath());
                } else {
                    logger.warn("Failed to create database directory: {}", dbDir.getAbsolutePath());
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create database directory for path: " + dbPath, e);
        }
    }
}
