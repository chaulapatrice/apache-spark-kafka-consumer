---
title: Upcoming launches to the Moon
---

```sql launches
SELECT 
    id,
    '/launch/' || id as launch_url,
    name,
    status__name,
    net,
FROM launch_api.upcoming_launches_to_moon
ORDER BY net
```

<DataTable data={launches} rows=15 link=launch_url rowShading=true rowLines=false>
  <Column id=name title="Name" />
  <Column id=status__name title="Status" />
  <Column id=net title="Not earlier than" />
</DataTable>

