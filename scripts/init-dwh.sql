-- Создаем схемы для слоев данных
CREATE SCHEMA IF NOT EXISTS raw;      -- Сырые данные из API
CREATE SCHEMA IF NOT EXISTS staging;  -- Очищенные данные
CREATE SCHEMA IF NOT EXISTS marts;    -- Готовые витрины
CREATE SCHEMA IF NOT EXISTS analytics;-- Аналитические модели

-- Комментируем для документации
COMMENT ON SCHEMA raw IS 'Raw data from external sources (API, files)';
COMMENT ON SCHEMA staging IS 'Cleaned, validated, transformed data';
COMMENT ON SCHEMA marts IS 'Business-ready data marts for reporting';
COMMENT ON SCHEMA analytics IS 'Analytical models and aggregates';