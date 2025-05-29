# S3 Inventory Analysis Tool

A web-based tool for analyzing and searching S3 inventory data. This tool helps you efficiently search through and analyze large S3 buckets by leveraging S3 inventory reports.

## Features

- 🔍 Search for objects and folders in S3 buckets
- 📊 Calculate sizes at any path depth
- 📈 View detailed size and object count statistics
- 🔄 Automatic manifest caching for faster repeated searches
- 🎯 Support for multiple buckets and manifests
- 🔐 Secure AWS credential handling

## Prerequisites

- Python 3.8 or higher
- AWS credentials with access to S3 buckets
- S3 inventory configured on the buckets you want to analyze

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/s3-inventory-analysis.git
cd s3-inventory-analysis
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file in the project root with your AWS credentials:
```
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_SESSION_TOKEN=your_session_token  # Optional
```

## Usage

1. Start the application:
```bash
python app.py
```

2. Open your web browser and navigate to `http://localhost:5000`

3. Select your S3 buckets and manifests

4. Choose your operation:
   - Search for String: Find objects or folders matching a search string
   - Calculate Path Depth Size: Analyze sizes at a specific path depth

## Project Structure

```
s3-inventory-analysis/
├── app.py                 # Main Flask application
├── s3_utils.py           # S3 client initialization and utilities
├── s3_inventory_utils.py # S3 inventory manifest handling
├── s3_inventory_search.py # Search functionality
├── s3_path_size.py       # Path depth size calculation
├── templates/
│   └── index.html        # Web interface
├── static/               # Static assets
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

## Development

### Setting up a development environment

1. Install development dependencies:
```bash
pip install -r requirements-dev.txt
```

2. Run tests:
```bash
pytest
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- AWS S3 Inventory for providing the data source
- Flask for the web framework
- Pandas for data manipulation
- TailwindCSS for the UI

## Support

If you encounter any issues or have questions, please:
1. Check the [existing issues](https://github.com/yourusername/s3-inventory-analysis/issues)
2. Create a new issue if your problem hasn't been reported 