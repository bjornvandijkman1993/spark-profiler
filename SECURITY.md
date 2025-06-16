# Security Policy

## Supported Versions

We take security seriously and aim to promptly address any security vulnerabilities in pyspark-analyzer.

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1   | :x:                |

## Reporting a Vulnerability

If you discover a security vulnerability in pyspark-analyzer, please report it through one of the following channels:

### Preferred Method: GitHub Security Advisory

1. Go to the [Security tab](https://github.com/bjornvandijkman1993/pyspark-analyzer/security) in our GitHub repository
2. Click on "Report a vulnerability"
3. Fill out the security advisory form with:
   - A description of the vulnerability
   - Steps to reproduce the issue
   - Potential impact
   - Any suggested fixes (if applicable)

### Alternative Method: Email

If you prefer to report via email, send details to: bjornvandijkman@gmail.com

Please include:
- Type of vulnerability
- Affected versions
- Steps to reproduce
- Potential impact
- Any proof-of-concept code

## What to Expect

- **Acknowledgment**: We will acknowledge receipt of your vulnerability report within 48 hours
- **Assessment**: We will assess the vulnerability and determine its severity within 5 business days
- **Updates**: We will keep you informed about the progress of addressing the vulnerability
- **Fix Timeline**:
  - Critical vulnerabilities: Within 7 days
  - High severity: Within 14 days
  - Medium severity: Within 30 days
  - Low severity: Next regular release

## Security Measures

This project implements comprehensive security measures:

### Dependency Scanning
- **Safety**: Checks Python dependencies for known vulnerabilities
- **Pip-audit**: Additional vulnerability scanning with detailed descriptions
- **Dependabot**: Automated dependency updates with security patches
- **OWASP Dependency Check**: Deep vulnerability analysis (weekly scans)
- **License Compliance**: Automated license compatibility checking

### Code Analysis
- **Bandit**: Static security analysis for Python code
- **Custom Bandit Plugins**: Spark-specific security checks including:
  - SQL injection detection in Spark SQL
  - Unsafe deserialization detection
  - Sensitive data exposure prevention
  - Unencrypted data transfer warnings
- **CodeQL**: Advanced semantic code analysis by GitHub
- **Pre-commit hooks**: Security checks before code is committed

### Secret Detection
- **detect-secrets**: Prevents secrets from being committed
- **Pre-commit secret scanning**: Catches secrets before they enter the repository
- **Baseline tracking**: Known false positives are tracked

### Supply Chain Security
- **SBOM Generation**: Software Bill of Materials in JSON/XML formats
- **Dependency pinning**: Exact versions specified in uv.lock
- **Vulnerability tracking**: Automated issue creation for high-severity vulnerabilities

### CI/CD Security
- Automated security scans on every pull request
- Security scan results published as GitHub summaries
- Failing builds on high-severity vulnerabilities
- Security reports uploaded as artifacts
- Automated PR comments with security findings

### Testing
- **Security test suite**: Dedicated security tests including:
  - Input validation and sanitization
  - SQL injection prevention
  - Safe data handling
  - Memory exhaustion prevention
  - Concurrent access safety
  - Data privacy verification

### Best Practices
- No hardcoded credentials or secrets
- Input validation for all user-provided data
- Secure defaults for all configurations
- Regular dependency updates
- Security advisory template for vulnerability disclosure

## Security Configuration

Security tools are configured with the following files:
- `.bandit`: Bandit security scanner configuration
- `.bandit-plugins/`: Custom Spark security rules
- `.safety-policy.json`: Safety vulnerability scanner policy
- `.secrets.baseline`: detect-secrets baseline configuration
- `.github/workflows/ci.yml`: Main CI security scanning
- `.github/workflows/dependency-check.yml`: OWASP weekly scans
- `.github/workflows/codeql.yml`: CodeQL analysis
- `.github/workflows/security-comment.yml`: PR security feedback
- `.github/dependency-check-suppression.xml`: OWASP false positive suppressions
- `.github/SECURITY_ADVISORY_TEMPLATE.md`: Template for security advisories

## Disclosure Policy

- We will publicly disclose the vulnerability after a fix is available
- We will credit reporters who wish to be acknowledged
- We request a 90-day disclosure embargo for critical vulnerabilities

## Contact

For any security-related questions or concerns, contact:
- Email: bjornvandijkman@gmail.com
- GitHub Security Advisories: [Report a vulnerability](https://github.com/bjornvandijkman1993/pyspark-analyzer/security/advisories/new)

Thank you for helping keep pyspark-analyzer secure!
