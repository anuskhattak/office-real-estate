-- Real Estate Database Schema
-- PostgreSQL | Load order: parent tables first (FK constraints)

-- 1. No foreign keys (parents)
CREATE TABLE agencies (
    agency_id      VARCHAR(20)  PRIMARY KEY,
    agency_name    VARCHAR(100) NOT NULL,
    city           VARCHAR(100),
    state          VARCHAR(100),
    phone          VARCHAR(30),
    founded_year   INT,
    license_number VARCHAR(50)
);

CREATE TABLE locations (
    location_id    VARCHAR(20) PRIMARY KEY,
    street_address VARCHAR(200),
    city           VARCHAR(100),
    state          VARCHAR(10),
    zip_code       VARCHAR(20),
    county         VARCHAR(100),
    latitude       NUMERIC(10, 6),
    longitude      NUMERIC(10, 6),
    metro_area     VARCHAR(100)
);

CREATE TABLE buyers (
    buyer_id            VARCHAR(20) PRIMARY KEY,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    email               VARCHAR(150),
    phone               VARCHAR(50),
    buyer_type          VARCHAR(50),
    pre_approval_amount NUMERIC(12, 2),
    registration_date   DATE
);

-- 2. Depends on agencies
CREATE TABLE agents (
    agent_id        VARCHAR(20) PRIMARY KEY,
    agency_id       VARCHAR(20) REFERENCES agencies(agency_id),
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(150),
    phone           VARCHAR(30),
    license_number  VARCHAR(50),
    hire_date       DATE,
    is_active       BOOLEAN,
    commission_rate NUMERIC(6, 4)
);

-- 3. Depends on locations
CREATE TABLE properties (
    property_id    VARCHAR(20) PRIMARY KEY,
    location_id    VARCHAR(20) REFERENCES locations(location_id),
    property_type  VARCHAR(50),
    year_built     INT,
    square_footage INT,
    bedrooms       INT,
    bathrooms      NUMERIC(4, 1),
    lot_size_sqft  NUMERIC(12, 2),
    garage_spaces  INT,
    has_pool       BOOLEAN,
    has_basement   BOOLEAN,
    stories        INT
);

-- 4. Depends on properties
CREATE TABLE property_features (
    feature_id    VARCHAR(20) PRIMARY KEY,
    property_id   VARCHAR(20) REFERENCES properties(property_id),
    feature_name  VARCHAR(100),
    feature_value VARCHAR(100)
);

-- 5. Depends on properties + agents
CREATE TABLE listings (
    listing_id     VARCHAR(20) PRIMARY KEY,
    property_id    VARCHAR(20) REFERENCES properties(property_id),
    agent_id       VARCHAR(20) REFERENCES agents(agent_id),
    list_price     NUMERIC(12, 2),
    list_date      DATE,
    expiry_date    DATE,
    listing_status VARCHAR(50),
    days_on_market INT,
    description    TEXT
);

-- 6. Depends on listings + buyers + agents
CREATE TABLE transactions (
    transaction_id     VARCHAR(20) PRIMARY KEY,
    listing_id         VARCHAR(20) REFERENCES listings(listing_id),
    buyer_id           VARCHAR(20) REFERENCES buyers(buyer_id),
    agent_id           VARCHAR(20) REFERENCES agents(agent_id),
    sale_price         NUMERIC(12, 2),
    sale_date          DATE,
    closing_date       DATE,
    commission_amount  NUMERIC(12, 2),
    transaction_status VARCHAR(50),
    financing_type     VARCHAR(50),
    inspection_passed  BOOLEAN,
    days_to_close      INT,
    commission_rate    NUMERIC(10, 6)
);
