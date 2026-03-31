DROP TABLE IF EXISTS fraud_labels CASCADE;
DROP TABLE IF EXISTS mcc_codes CASCADE;
DROP TABLE IF EXISTS cards CASCADE;
DROP TABLE IF EXISTS users CASCADE;

CREATE TABLE users (
    id BIGINT PRIMARY KEY,
    current_age INT,
    retirement_age INT,
    birth_year INT,
    birth_month INT,
    gender VARCHAR(10),
    address VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    per_capita_income VARCHAR(50),
    yearly_income VARCHAR(50),
    total_debt VARCHAR(50),
    credit_score INT,
    num_credit_cards INT
);

CREATE TABLE cards (
    id BIGINT,
    client_id BIGINT,
    card_brand VARCHAR(50),
    card_type VARCHAR(50),
    card_number BIGINT,
    expires VARCHAR(10),
    cvv INT,
    has_chip VARCHAR(5),
    num_cards_issued INT,
    credit_limit VARCHAR(50),
    acct_open_date VARCHAR(10),
    year_pin_last_changed INT,
    card_on_dark_web VARCHAR(5),
    PRIMARY KEY (id, client_id)
);

CREATE TABLE mcc_codes (
    mcc VARCHAR(10) PRIMARY KEY,
    description TEXT
);

CREATE TABLE fraud_labels (
    transaction_id BIGINT PRIMARY KEY,
    is_fraud VARCHAR(10)
);

CREATE INDEX idx_users_id ON users(id);
CREATE INDEX idx_cards_client_id ON cards(client_id);
CREATE INDEX idx_cards_id ON cards(id);
CREATE INDEX idx_mcc_code ON mcc_codes(mcc);
CREATE INDEX idx_fraud_transaction_id ON fraud_labels(transaction_id);
