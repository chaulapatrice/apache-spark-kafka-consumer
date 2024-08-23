SELECT
    "ul"."id",
    "ul"."name",
    "ul"."status__name",
    "ul"."status__description",
    "ul"."last_updated",
    "ul"."net",
    "ul"."window_start",
    "ul"."window_end",
    "ul"."launch_service_provider__name" AS "agency__name",
    (
        SELECT
            "logo_url"
        FROM
            "upcoming_launches"."upcoming_launches__mission__agencies"
        WHERE
            "id" = "ul"."launch_service_provider__id"
    ) AS "agency__logo_url",
    "ul"."rocket__configuration__full_name",
    "ul"."mission__name",
    "ul"."mission__description",
    "ul"."mission__type",
    "ul"."pad__map_url",
    "ul"."pad__location__name",
    "ul"."image"
FROM
    "upcoming_launches"."upcoming_launches" AS "ul"
WHERE
    mission__type IN (
        'Lunar Exploration',
        'Planetary Science',
        'Tourism'
    )
    AND (
        mission__description ILIKE '%lunar%'
        OR mission__description ILIKE '%moon%'
    )
GROUP BY
    "ul"."id"