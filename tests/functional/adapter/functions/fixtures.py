DATABRICKS_PYTHON_UDF_BODY = """
def price_for_xlarge(price):
    return price * 2

return price_for_xlarge(price)
"""

DATABRICKS_PYTHON_UDF_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      entry_point: price_for_xlarge
      runtime_version: "3.11"
    arguments:
      - name: price
        data_type: float
        description: The price of the standard item
    returns:
      data_type: float
      description: The resulting xlarge price
"""


DATABRICKS_MULTI_ARG_PYTHON_UDF_BODY = """
def total_price(price, quantity):
    return price * quantity

return total_price(price, quantity)
"""

DATABRICKS_MULTI_ARG_PYTHON_UDF_YML = """
functions:
  - name: total_price
    description: Calculate total price from unit price and quantity
    arguments:
      - name: price
        data_type: float
      - name: quantity
        data_type: int
    returns:
      data_type: float
"""


PYTHON_UDF_V1 = """
return price * 2
"""

PYTHON_UDF_V2 = """
return price * 3
"""

PYTHON_UDF_YML_V1 = """
functions:
  - name: price_for_xlarge
    config:
      entry_point: price_for_xlarge
      runtime_version: "3.11"
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
"""
