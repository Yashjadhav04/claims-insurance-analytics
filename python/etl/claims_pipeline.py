"""
Claims & Insurance Analytics ETL Pipeline

Orchestrates the complete ETL process for claims data:
- Extract from multiple claims systems
- Transform and validate data
- Load into analytics warehouse
- Generate quality reports

Author: Yash Jadhav
Last Updated: October 2026
"""

import logging
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
import hashlib

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text
import numpy as np
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

load_dotenv()


@dataclass
class DataQualityReport:
    """Data quality metrics report"""
    timestamp: str
    total_records: int
    valid_records: int
    invalid_records: int
    null_count_by_field: Dict[str, int]
    duplicate_count: int
    outlier_count: int
    validation_passed: bool
    quality_score: float


class ClaimsETLPipeline:
    """Main ETL pipeline for claims data"""

    def __init__(self, env: str = 'prod'):
        """
        Initialize ETL pipeline

        Args:
            env: Environment (dev, staging, prod)
        """
        self.env = env
        self.run_id = self._generate_run_id()
        self.engine = self._create_db_connection()
        self.quality_report = None

        logger.info(f"Pipeline initialized: {self.run_id}")

    @staticmethod
    def _generate_run_id() -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        hash_id = hashlib.md5(f"{timestamp}{os.getpid()}".encode()).hexdigest()[:8]
        return f"PIPELINE_{timestamp}_{hash_id}"

    def _create_db_connection(self):
        """Create database connection"""
        db_url = os.getenv('DATABASE_URL')
        return create_engine(db_url)

    def run(self) -> Dict:
        """Execute complete ETL pipeline"""
        logger.info(f"Starting ETL pipeline: {self.run_id}")
        start_time = datetime.now()

        try:
            # Step 1: Extract
            logger.info("Step 1/5: Extracting claims data...")
            claims_raw = self._extract_claims()
            services_raw = self._extract_services()
            claimants_raw = self._extract_claimants()

            # Step 2: Validate Raw Data
            logger.info("Step 2/5: Validating raw data...")
            self._validate_raw_data(claims_raw, services_raw, claimants_raw)

            # Step 3: Transform
            logger.info("Step 3/5: Transforming data...")
            claims_clean = self._transform_claims(claims_raw)
            services_clean = self._transform_services(services_raw)
            claimants_clean = self._transform_claimants(claimants_raw)

            # Step 4: Load to Warehouse
            logger.info("Step 4/5: Loading to warehouse...")
            self._load_to_warehouse(claims_clean, services_clean, claimants_clean)

            # Step 5: Post-Load Validation
            logger.info("Step 5/5: Post-load validation...")
            self._validate_loaded_data()

            execution_time = datetime.now() - start_time

            result = {
                "run_id": self.run_id,
                "status": "SUCCESS",
                "execution_time_seconds": execution_time.total_seconds(),
                "records_processed": {
                    "claims": len(claims_clean),
                    "services": len(services_clean),
                    "claimants": len(claimants_clean)
                },
                "quality_report": asdict(self.quality_report) if self.quality_report else {}
            }

            logger.info(f"Pipeline completed successfully in {execution_time}")
            return result

        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            return {
                "run_id": self.run_id,
                "status": "FAILED",
                "error": str(e)
            }

        finally:
            if self.engine:
                self.engine.dispose()

    def _extract_claims(self) -> pd.DataFrame:
        """Extract claims from source system"""
        query = """
        SELECT
            claim_id,
            claimant_id,
            claim_date,
            claim_amount,
            status,
            category,
            provider_id,
            claim_type,
            resolution_date,
            created_at,
            updated_at
        FROM raw.claims
        WHERE created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAYS)
            OR updated_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        """

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        logger.info(f"Extracted {len(df)} claims records")
        return df

    def _extract_services(self) -> pd.DataFrame:
        """Extract services/procedures from claims"""
        query = """
        SELECT
            service_id,
            claim_id,
            service_date,
            service_type,
            cost,
            quantity,
            provider_specialty,
            approved_amount,
            created_at
        FROM raw.claim_services
        WHERE created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAYS)
        """

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        logger.info(f"Extracted {len(df)} service records")
        return df

    def _extract_claimants(self) -> pd.DataFrame:
        """Extract claimant master data"""
        query = """
        SELECT
            claimant_id,
            claimant_name,
            age,
            gender,
            region,
            enrollment_date,
            membership_type,
            status,
            created_at,
            updated_at
        FROM raw.claimants
        """

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        logger.info(f"Extracted {len(df)} claimant records")
        return df

    def _validate_raw_data(self, claims: pd.DataFrame, services: pd.DataFrame, claimants: pd.DataFrame) -> None:
        """Validate raw extracted data"""
        issues = []

        # Claims validation
        if claims['claim_id'].isna().any():
            issues.append(f"Claims: {claims['claim_id'].isna().sum()} null claim_ids")

        if (claims['claim_amount'] < 0).any():
            issues.append(f"Claims: {(claims['claim_amount'] < 0).sum()} negative amounts")

        # Services validation
        if services['claim_id'].isin(claims['claim_id']).sum() < len(services) * 0.95:
            issues.append("Services: Some services reference non-existent claims")

        # Claimants validation
        if claimants['claimant_id'].duplicated().any():
            dups = claimants['claimant_id'].duplicated().sum()
            issues.append(f"Claimants: {dups} duplicate claimant_ids")

        if issues:
            logger.warning("Raw data validation issues:")
            for issue in issues:
                logger.warning(f"  - {issue}")

    def _transform_claims(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform claims data"""
        df = df.copy()

        # Data type conversions
        df['claim_date'] = pd.to_datetime(df['claim_date'])
        df['resolution_date'] = pd.to_datetime(df['resolution_date'])
        df['claim_amount'] = pd.to_numeric(df['claim_amount'], errors='coerce')

        # Remove invalid records
        df = df.dropna(subset=['claim_id', 'claim_date', 'claim_amount'])

        # Standardize status values
        status_mapping = {
            'APPROVED': 'Approved',
            'DENIED': 'Denied',
            'PENDING': 'Pending',
            'WITHDRAWN': 'Withdrawn'
        }
        df['status'] = df['status'].str.upper().map(status_mapping)
        df['status'].fillna('Pending', inplace=True)

        # Calculate processing time
        df['days_to_resolve'] = (df['resolution_date'] - df['claim_date']).dt.days

        # Add processing metadata
        df['processed_at'] = datetime.now()
        df['run_id'] = self.run_id
        df['data_source'] = 'claims_system'

        logger.info(f"Transformed {len(df)} claims records")
        return df

    def _transform_services(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform services data"""
        df = df.copy()

        df['service_date'] = pd.to_datetime(df['service_date'])
        df['cost'] = pd.to_numeric(df['cost'], errors='coerce')
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(1)

        df = df.dropna(subset=['service_id', 'claim_id', 'cost'])

        # Calculate total cost
        df['total_cost'] = df['cost'] * df['quantity']

        df['processed_at'] = datetime.now()
        df['run_id'] = self.run_id

        logger.info(f"Transformed {len(df)} service records")
        return df

    def _transform_claimants(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform claimant data"""
        df = df.copy()

        df['enrollment_date'] = pd.to_datetime(df['enrollment_date'])
        df['age'] = pd.to_numeric(df['age'], errors='coerce')

        df = df.dropna(subset=['claimant_id'])

        # Standardize gender
        df['gender'] = df['gender'].str.upper()
        df.loc[~df['gender'].isin(['M', 'F', 'O']), 'gender'] = 'O'

        df['processed_at'] = datetime.now()
        df['run_id'] = self.run_id

        logger.info(f"Transformed {len(df)} claimant records")
        return df

    def _load_to_warehouse(self, claims: pd.DataFrame, services: pd.DataFrame, claimants: pd.DataFrame) -> None:
        """Load data to data warehouse"""
        with self.engine.connect() as conn:
            # Load to staging tables
            claims.to_sql('stg_claims', conn, schema='staging', if_exists='append', index=False)
            services.to_sql('stg_claim_services', conn, schema='staging', if_exists='append', index=False)
            claimants.to_sql('stg_claimants', conn, schema='staging', if_exists='append', index=False)

            conn.commit()

        logger.info("Data loaded to warehouse staging tables")

    def _validate_loaded_data(self) -> None:
        """Validate data after loading"""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT
                    COUNT(*) as row_count,
                    COUNT(DISTINCT claim_id) as unique_claims
                FROM staging.stg_claims
                WHERE run_id = :run_id
            """), {"run_id": self.run_id})

            row = result.fetchone()
            logger.info(f"Post-load validation: {row[0]} rows, {row[1]} unique claims")

    def generate_quality_report(self, df: pd.DataFrame, table_name: str) -> DataQualityReport:
        """Generate data quality report"""
        total = len(df)

        # Count nulls by field
        null_counts = df.isna().sum().to_dict()

        # Count duplicates
        duplicates = df.duplicated().sum()

        # Count outliers (simplified)
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        outliers = 0
        for col in numeric_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            outliers += ((df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))).sum()

        valid_records = total - duplicates
        quality_score = (valid_records / total * 100) if total > 0 else 0

        report = DataQualityReport(
            timestamp=datetime.now().isoformat(),
            total_records=total,
            valid_records=valid_records,
            invalid_records=duplicates,
            null_count_by_field=null_counts,
            duplicate_count=duplicates,
            outlier_count=outliers,
            validation_passed=quality_score > 95,
            quality_score=quality_score
        )

        self.quality_report = report
        return report


def main():
    """Main entry point"""
    env = os.getenv('ENVIRONMENT', 'prod')
    pipeline = ClaimsETLPipeline(env=env)
    result = pipeline.run()

    logger.info(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))

    return result


if __name__ == "__main__":
    main()

