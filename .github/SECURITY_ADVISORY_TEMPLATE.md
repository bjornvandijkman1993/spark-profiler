# Security Advisory Template

**Advisory ID**: GHSA-XXXX-XXXX-XXXX
**Product**: pyspark-analyzer
**Affected Versions**: [e.g., < 1.2.3]
**Patched Versions**: [e.g., >= 1.2.3]
**Severity**: [Critical/High/Medium/Low]
**CVE ID**: CVE-YYYY-NNNNN (if applicable)
**CWE ID**: [e.g., CWE-89 SQL Injection]

## Summary
[Brief description of the vulnerability - 1-2 sentences]

## Description
[Detailed description of the vulnerability, including:
- What the vulnerability is
- How it can be exploited
- What the impact is]

## Affected Components
- Component: [e.g., DataFrameProfiler.profile()]
- File(s): [e.g., pyspark_analyzer/profiler.py]
- Function(s): [specific functions affected]

## Vulnerability Details
### Attack Vector
[Describe how an attacker could exploit this vulnerability]

### Prerequisites
[What conditions must be met for the vulnerability to be exploitable]

### Impact
- **Confidentiality Impact**: [None/Low/High]
- **Integrity Impact**: [None/Low/High]
- **Availability Impact**: [None/Low/High]

## Proof of Concept
```python
# Example code demonstrating the vulnerability
# DO NOT include actual exploit code that could be harmful
```

## Mitigation
### For Users
[Steps users can take to mitigate the vulnerability before patching]

### For Developers
[How the vulnerability was fixed in the code]

## Timeline
- **Discovery Date**: YYYY-MM-DD
- **Reported to Maintainers**: YYYY-MM-DD
- **Public Disclosure**: YYYY-MM-DD
- **Patch Released**: YYYY-MM-DD

## Credit
Discovered by: [Name/Handle] ([Organization])

## References
- [Link to patch/PR]
- [Link to issue]
- [External references]

## CVSS Score
**Base Score**: X.X ([Critical/High/Medium/Low])
**Vector String**: CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H

---

## For Maintainers Only

### Checklist
- [ ] Verify the vulnerability
- [ ] Develop and test patch
- [ ] Update version numbers
- [ ] Create GitHub Security Advisory
- [ ] Request CVE (if applicable)
- [ ] Notify users via:
  - [ ] GitHub Security Advisory
  - [ ] Email to security mailing list
  - [ ] Social media announcement
- [ ] Update documentation
- [ ] Add regression tests
