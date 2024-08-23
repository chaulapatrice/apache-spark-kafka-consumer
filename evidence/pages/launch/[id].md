```sql launch
SELECT
    *
FROM
    launch_api.upcoming_launches_to_moon
WHERE
    "id" = '${params.id}'
```

# {launch[0].name}

## Service Provider
<div class="border border-solid border-gray-200 p-3 mt-3">
    <p class="my-3 font-bold text-xm">
     {launch[0].agency__name}
    </p>
    <p class="my-3">
        <img src="{launch[0].agency__logo_url}" style="height:100px" alt="{launch[0].agency__name}" /> 
    </p>
</div>

## Time and Location
<div class="border border-solid border-gray-200 p-3 mt-3">
    <Grid cols=3>
        <div>
            <p>
                <b>Not earlier than</b>
            </p>

            <p class="mt-2">
                {launch[0].net}
            </p>
        </div>
  
        <div>
            <p>
                <b>Window start</b>
            </p>

            <p class="mt-2">
                {launch[0].window_start}
            </p>
        </div>

        <div>
            <p>
                <b>Window end</b>
            </p>

            <p class="mt-2">
                {launch[0].window_end}
            </p>
        </div>

        <div>
            <p>
                <b>Pad location name</b>
            </p>

            <p class="mt-2">
                {launch[0].pad__location__name}
            </p>
        </div>
    </Grid>
    
    <LinkButton url="{launch[0].pad__map_url}">
        View Pad Location
    </LinkButton>
</div>

## Rocket
<div class="border border-solid border-gray-200 p-3 mt-3">
    <p class="my-3 font-bold text-xm">
        {launch[0].rocket__configuration__full_name}
    </p>

    <p>
        <img src="{launch[0].image}" alt="{launch[0].rocket__configuration__full_name}"/> 
    </p>
</div>


## Mission
<div class="border border-solid border-gray-200 p-3 mt-3">
    <Grid cols=2>
        <div>
            <p>
                <b>Mission name</b>
            </p>

            <p class="mt-2">
                {launch[0].mission__name}
            </p>
        </div>

        <div>
            <p>
                <b>Mission type</b>
            </p>

            <p class="mt-2">
                {launch[0].mission__type}
            </p>
        </div>
     
    </Grid>

    <p class="mt-5">
     {launch[0].mission__description}
    </p>
</div>
