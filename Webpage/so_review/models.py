"""Mappings for existing shared tables only. Never call create_all here."""
from flask import current_app, g
from sqlalchemy import Boolean, Column, DateTime, Integer, JSON, String, Uuid
from sqlalchemy.orm import declarative_base, sessionmaker


class Database:
    @property
    def engine(self):
        return current_app.extensions['so_review_engine']

    @property
    def session(self):
        if 'so_review_session' not in g:
            g.so_review_session = sessionmaker(bind=self.engine)()
        return g.so_review_session


db = Database()


class Query:
    def __get__(self, instance, cls):
        return db.session.query(cls)


Base = declarative_base()


class ApprovedSO(Base):
    __tablename__ = 'approved_work_orders'
    query = Query()
    sales_order = Column(String(80), primary_key=True)
    customer = Column(String(200))
    customer_po = Column(String(120))
    items = Column(JSON, nullable=False, default=list)
    metadata_fields = Column(JSON, nullable=False, default=dict)
    previous_snapshot = Column(JSON)
    latest_snapshot = Column(JSON)
    approved_snapshot = Column(JSON)
    google_present = Column(Boolean, nullable=False, default=False)
    is_active = Column(Boolean, nullable=False, default=False)
    synced_at = Column(DateTime)
    approved_at = Column(DateTime)
    source_revision = Column(Integer, nullable=False, default=0)
    revision = Column(Integer, nullable=False, default=0)
    workflow_status = Column(String(40), nullable=False, default='needs_material_review')
    returned_from_mes = Column(Boolean, nullable=False, default=False)
    approved_order_signature = Column(JSON)
    mes_released_at = Column(DateTime(timezone=True))
    review_incident_id = Column(Uuid)
    material_review_id = Column(Uuid)
    reconciliation_required = Column(Boolean, nullable=False, default=False)
    reconciliation_details = Column(JSON, nullable=False, default=dict)

    def to_source(self):
        return dict(self.metadata_fields or {}, sales_order=self.sales_order,
                    customer=self.customer, customer_po=self.customer_po,
                    items=self.items, revision=self.revision)


class SalesOrder(Base):
    __tablename__ = 'WO Details'
    query = Query()
    id = Column(String, primary_key=True)
    sales_order = Column(String(80))
    record_source = Column(String(20))
    items = Column(JSON)

    @classmethod
    def mes_query(cls):
        return cls.query.filter_by(record_source='mes')
