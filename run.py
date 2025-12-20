#!/usr/bin/env python3
"""Точка входа в приложение"""

import sys
from pathlib import Path

# Добавляем корень проекта в путь
sys.path.insert(0, str(Path(__file__).parent))

from scripts.main import main

if __name__ == "__main__":
    sys.exit(main())