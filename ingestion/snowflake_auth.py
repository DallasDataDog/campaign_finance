def sf_auth (schema_name):
    # Import necessary libraries
    import yaml
    import snowflake.connector
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    with open('authentication.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # Snowflake connection parameters
    USER = config['snowflake']['user'] #the value here should match with your Snowflake LOGIN_NAME
    ACCOUNT = config['snowflake']['account']
    ROLE = config['snowflake']['role']
    DATABASE = config['snowflake']['database']
    SCHEMA = schema_name
    PRIVATE_KEY_PATH = config['snowflake']['private_key_path']  # Path to your private key, stored on host machine

    # Function to load the private key
    def load_private_key(private_key_path):
        with open(private_key_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )
        pkb = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        return pkb

    conn = snowflake.connector.connect(
        user=USER,
        account=ACCOUNT,
        role=ROLE,
        database=DATABASE,
        schema=SCHEMA,
        private_key=load_private_key(PRIVATE_KEY_PATH)
    )
    return conn


def sf_upload_file_to_stage(conn, directory, file_name, stage_name):
    import os
    file_path = os.path.join(directory, file_name)
    conn.cursor().execute(f"PUT file://{file_path} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;")
    print(f"Uploaded {file_name} to stage @{stage_name}")
    #os.remove(file_path)
