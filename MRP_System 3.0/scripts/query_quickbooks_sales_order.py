"""Read original SO line order from an open QuickBooks Desktop company file.

Run in a normal interactive terminal in the same Windows session as QuickBooks.
This script sends only SalesOrderQuery; it never modifies QuickBooks data.
"""
import argparse
import sys
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ref-number", default="SO-20261123", help="Exact QuickBooks sales-order reference number")
    args = parser.parse_args()
    try:
        import win32com.client
    except ImportError:
        print("Install the Windows dependency: python -m pip install pywin32", file=sys.stderr)
        return 1

    processor = None
    ticket = None
    connected = False
    try:
        processor = win32com.client.Dispatch("QBXMLRP2.RequestProcessor")
        processor.OpenConnection2("", "MRP Sales Order Reader", 1)
        connected = True
        print("Connecting to the open company file; approve read access if prompted.", flush=True)
        ticket = processor.BeginSession("", 2)
        request = f'''<?xml version="1.0"?><?qbxml version="13.0"?>
<QBXML><QBXMLMsgsRq onError="stopOnError">
<SalesOrderQueryRq requestID="1"><RefNumber>{escape(args.ref_number)}</RefNumber>
<IncludeLineItems>true</IncludeLineItems></SalesOrderQueryRq>
</QBXMLMsgsRq></QBXML>'''
        root = ET.fromstring(processor.ProcessRequest(ticket, request))
        response = root.find(".//SalesOrderQueryRs")
        if response is None:
            raise RuntimeError("Missing SalesOrderQuery response.")
        print("Status:", response.get("statusCode"), response.get("statusMessage"))
        if response.get("statusSeverity") == "Error":
            return 1
        orders = response.findall("SalesOrderRet")
        if not orders:
            print("No matching SO. Check the exact reference number in QuickBooks.")
        for order in orders:
            print("Sales Order:", order.findtext("RefNumber"))
            for position, line in enumerate(order.iter("SalesOrderLineRet"), 1):
                print(position, line.findtext("TxnLineID"), line.findtext("ItemRef/FullName"), line.findtext("Quantity"), sep=" | ")
        return 0
    except Exception as exc:
        print("QuickBooks connection/query failed:", str(exc), file=sys.stderr)
        return 1
    finally:
        try:
            if ticket is not None:
                processor.EndSession(ticket)
        finally:
            if connected:
                processor.CloseConnection()


if __name__ == "__main__":
    raise SystemExit(main())
