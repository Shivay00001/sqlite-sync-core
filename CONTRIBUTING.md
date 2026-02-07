# Contributing to sqlite-sync-core

Thank you for your interest in contributing to sqlite-sync-core! This document provides guidelines for contributing.

## Code of Conduct

Be respectful and constructive. We welcome contributors of all experience levels.

## How to Contribute

### Reporting Bugs

1. Check if the issue already exists
2. Create a new issue with:
   - Clear title
   - Steps to reproduce
   - Expected vs actual behavior
   - Python version and OS

### Suggesting Features

1. Open an issue with `[Feature]` prefix
2. Describe the use case
3. Propose implementation if possible

### Pull Requests

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes
4. Run tests: `pytest tests/ -v`
5. Run type checking: `mypy src/`
6. Commit with clear message
7. Push and create PR

## Development Setup

```bash
git clone https://github.com/shivay00001/sqlite-sync-core.git
cd sqlite-sync-core
pip install -e .[dev]
pytest tests/ -v
```

## Code Style

- Follow PEP 8
- Use type hints
- Write docstrings for public functions
- Keep functions focused and small

## Testing

- All new features must have tests
- Maintain or improve coverage
- Test edge cases and error conditions

## License

By contributing, you agree that your contributions will be licensed under AGPL-3.0.

---

Questions? Open an issue or contact: <shivaysinghrajput@proton.me>
