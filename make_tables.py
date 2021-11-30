#!/usr/bin/env python3

from csvbase.models import Base
from csvbase.app import engine

Base.metadata.create_all(bind=engine)
