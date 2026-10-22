from copy import deepcopy
import unittest
from mrp_system.so_workflow import order_signature, next_state, execution_limits, allocation_conflicts, restore_fulfilled_lines, REVIEW, PLANNING, MES, CLOSED
from mrp_system.order_staging import cli_sources
import pandas as pd


def source(qty=7, remaining=7, shipped=0):
    return {'ship_date':'2026-10-07','items':[{'id':'a','item':'System-A','quantity':qty,
            'ordered_quantity':qty,'remaining_quantity':remaining,'shipped_quantity':shipped}]}


class WorkflowStateTests(unittest.TestCase):
    def old(self, status=MES):
        return {'workflow_status':status,'approved_order_signature':order_signature(source()),
                'mes_released_at':'2026-10-01' if status==MES else None,'returned_from_mes':False,'source_present':True}

    def test_new_order_needs_review(self):
        self.assertEqual(next_state(None,source()),(REVIEW,False))

    def test_date_partial_shipment_case_and_ids_do_not_return(self):
        incoming=source(remaining=1,shipped=6)
        incoming.update(ship_date='2030-01-01',production_date='2026-10-15')
        incoming['items'][0].update(id='new-id',item=' system-a ',inventory_site='other',configuration='changed')
        self.assertEqual(next_state(self.old(),incoming),(MES,False))

    def test_item_replacement_and_ordered_quantity_changes_return(self):
        for incoming in (source(8),source(6),{'items':[]},{'items':[{'item':'System-B','quantity':7}]}):
            self.assertEqual(next_state(self.old(),incoming),(REVIEW,True))

    def test_changed_before_initial_release_has_no_returned_badge(self):
        self.assertEqual(next_state(self.old(PLANNING),source(8)),(REVIEW,False))

    def test_repeat_import_does_not_reopen_or_reset_returned_review(self):
        old=self.old(); old.update(workflow_status=REVIEW,returned_from_mes=True)
        self.assertEqual(next_state(old,source(8)),(REVIEW,True))
        self.assertEqual(next_state(old,source()),(REVIEW,True))

    def test_reappearing_archived_source_requires_review(self):
        old=self.old(CLOSED); old['source_present']=False
        self.assertEqual(next_state(old,source()),(REVIEW,False))

    def test_order_signature_ignores_nonproduction_lines_and_manual_extras(self):
        incoming=source()
        incoming['items'] += [{'item':'Engineer Services','quantity':1},{'item':'Shipping Charge','quantity':1},
                              {'id':'manual-x','item':'Extra','quantity':2}, {'item':' Tariff Charge ','quantity':1}]
        self.assertEqual(order_signature(incoming),order_signature(source()))

    def test_reordering_duplicate_lines_does_not_change_product_requirement(self):
        self.assertEqual(order_signature({'items':[{'item':'A','quantity':3},{'item':'A','quantity':4}]}),
                         order_signature({'items':[{'item':'A','quantity':7}]}))

    def test_cli_uses_existing_canonical_so_number_for_engineering_prefix(self):
        frame=pd.DataFrame([{'QB Num':'EO-20260002','Item':'A','Qty':1}])
        self.assertEqual(set(cli_sources(frame)),{'SO-20260002'})

    def test_cli_preserves_total_ordered_and_fulfillment_separately(self):
        frame=pd.DataFrame([{'QB Num':'SO1','Item':'System-A','Qty':1,'Ordered Qty':7,'Shipped Qty':6}])
        item=cli_sources(frame)['SO1']['items'][0]
        self.assertEqual((item['quantity'],item['ordered_quantity'],item['remaining_quantity'],item['shipped_quantity']),(7,7,1,6))
        with self.assertRaises(ValueError): cli_sources(frame.assign(**{'Ordered Qty':8}))

    def test_external_shipments_reduce_release_target(self):
        incoming=source(remaining=1,shipped=6)
        self.assertEqual(execution_limits(incoming['items'],[],incoming),{'system-a':1})

    def test_mes_shipments_not_subtracted_twice(self):
        incoming=source(remaining=1,shipped=6)
        wos=[{'is_shipped':True,'items':[{'item':'System-A','quantity':6}]}]
        self.assertEqual(execution_limits(incoming['items'],wos,incoming),{'system-a':7})

    def test_fully_shipped_line_omission_is_not_an_order_amendment(self):
        prior=source()
        proof=[{'items':[{'item':'System-A','quantity':7}],'is_shipped':True}]
        incoming=restore_fulfilled_lines({'items':[]},prior,proof)
        self.assertEqual(order_signature(incoming),order_signature(prior))
        self.assertEqual(incoming['items'][0]['remaining_quantity'],0)
        self.assertEqual(next_state(self.old(),incoming),(MES,False))

    def test_unproven_line_omission_still_requires_review(self):
        incoming=restore_fulfilled_lines({'items':[]},source(),[])
        self.assertEqual(next_state(self.old(),incoming),(REVIEW,True))

    def test_conflicts_are_explicit_not_silently_truncated(self):
        items=[{'item':'A','quantity':8}]
        saved=[{'item':'A','quantity':10}]
        self.assertEqual(allocation_conflicts(items,saved),[{'item':'a','ordered':8,'allocated':10}])
        self.assertEqual(saved[0]['quantity'],10)
