{% docs payment_transaction_type %}
	The category of transaction that occurs within a payment system. 
	- credit represents currency issued to a customer
	- debit represents currency used by a customer
{% enddocs %}

{% docs credit_type %}
    The classification of the credit event.  
    Possible values may include:
    - `manual` – Credit manually added by an admin.  
    - `refund` – Credit issued as a refund for a previous transaction.  
    - `reward` – Credit given as part of our loyalty program.  
    - `gift` – Credit added from a giftcard.  
{% enddocs %}
