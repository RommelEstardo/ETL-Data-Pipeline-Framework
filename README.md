# ETL Data Pipeline Framework

A robust, secure, and flexible ETL (Extract, Transform, Load) framework designed to automate data processing from multiple sources into SQL Server databases.

## 🌟 Features

- **Multiple Data Source Support**
  - SFTP servers
  - AWS S3 buckets
  - URL/Web endpoints
  - Local file systems

- **File Format Support**
  - CSV files
  - JSON files
  - ZIP archives (with automatic extraction)
  - Text files

- **Security Features**
  - AWS Systems Manager Parameter Store integration for secure credential management
  - TLS encryption for email notifications
  - Secure SFTP connections
  - Encrypted database connections

- **Import Methods**
  - BCP (Bulk Copy Program) for high-performance data loading
  - Pandas for flexible data manipulation
  - Bulk Insert for efficient data transfer

- **Additional Features**
  - Email notifications for process status
  - Detailed logging system
  - Automatic file archiving
  - Configurable batch processing
  - Error handling and recovery
  - Support for file header detection

## 📋 Prerequisites

- Python 3.x
- AWS Account with appropriate permissions
- SQL Server instance
- Required Python packages:
  ```
  boto3
  pandas
  pyodbc
  paramiko
  requests
  numpy
  sqlalchemy
  ```

## 🚀 Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/ETL-Data-Pipeline-Framework.git
   ```

2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure AWS credentials:
   ```bash
   aws configure
   ```

4. Set up your configuration files:
   - Copy and modify the appropriate .ini file from the config templates:
     - `config_local.ini` for local file processing
     - `config_s3.ini` for AWS S3 sources
     - `config_sftp.ini` for SFTP sources
     - `config_url.ini` for URL sources

## ⚙️ Configuration

### AWS Parameter Store Setup

1. Run `setAWSparameter.py` to set up secure credentials:
   ```bash
   python setAWSparameter.py
   ```

2. Configure the following parameters in AWS Systems Manager Parameter Store:
   - `sftp_password`
   - `smtp_password`
   - `sql_password`

### Configuration Files

Modify the appropriate .ini file for your use case:

```ini
[ETL]
data_source_type = sftp|s3|url|local
database_type = mssql
file_type = csv|json|txt
field_delimiter = ,|\t
file_has_header = True|False
...

[MSSQL]
server = your_server
database = your_database
user = your_username
table_name = your_table
...
```

## 🎯 Usage

### Local File Processing
```bash
python etlLocal.py
```

### S3 Source Processing
```bash
python etlS3.py
```

### SFTP Source Processing
```bash
python etlSFTP.py
```

### URL Source Processing
```bash
python etlURL.py
```

### Parameter Store Management
```bash
python manageParameterStore.py
```

## 📁 Project Structure

```
ETL-Data-Pipeline-Framework/
├── etlModule.py          # Core ETL functionality
├── etlLocal.py           # Local file processing
├── etlS3.py             # S3 source processing
├── etlSFTP.py           # SFTP source processing
├── etlURL.py            # URL source processing
├── manageParameterStore.py    # AWS Parameter Store management
├── setAWSparameter.py   # AWS Parameter Store setup
├── config/
│   ├── config_local.ini
│   ├── config_s3.ini
│   ├── config_sftp.ini
│   └── config_url.ini
└── README.md
```

## 🔒 Security Notes

- Credentials are stored securely in AWS Systems Manager Parameter Store
- All passwords and sensitive information are encrypted
- SFTP connections use secure protocols
- Email notifications use TLS encryption
- Database connections support encryption

## ⚠️ Important Considerations

- Ensure proper AWS IAM permissions are configured
- Regularly rotate credentials in AWS Parameter Store
- Monitor log files for process status and errors
- Maintain appropriate database permissions
- Regular backup of configuration files
- Test new configurations in a development environment first

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👤 Author

Rommel Estardo - [@RommelEstardo](https://github.com/RommelEstardo)

## 🙏 Acknowledgments

- AWS Documentation
- Microsoft SQL Server Documentation
- Python Community
- Open Source Contributors
