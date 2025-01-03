{% docs first_order_id %}
When attributed to a subscription, this represents the order that created the subscription.
When attributed to a customer, this represents the first order for the subscription.

In both instances, this order_id is not considered a subscription order and is only populated if the order is a completed order.
{% enddocs %}

{% docs order_status %}
The high-level status of the order. Possible values include:	
- return_to_sender
- in_transit
- failed_or_canceled
- pending
- delivered
- subscription_activated
{% enddocs %}