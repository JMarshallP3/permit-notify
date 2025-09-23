#!/usr/bin/env python3
"""
Script to check what's in the database.
"""

import os
import sys
from db.session import SessionLocal
from db.models import Permit

def main():
    session = SessionLocal()
    try:
        permits = session.query(Permit).all()
        print(f'Total permits in database: {len(permits)}')
        print('\nRecent permits:')
        for p in permits[-10:]:
            print(f'  {p.operator_name} - {p.lease_name} - {p.api_no}')
        
        print('\nPermits without API numbers:')
        no_api = session.query(Permit).filter(Permit.api_no.is_(None)).all()
        print(f'Count: {len(no_api)}')
        for p in no_api[:5]:
            print(f'  {p.operator_name} - {p.lease_name}')
            
    finally:
        session.close()

if __name__ == "__main__":
    main()
