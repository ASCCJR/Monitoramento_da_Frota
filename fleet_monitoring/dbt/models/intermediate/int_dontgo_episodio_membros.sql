with episode_gap_config as (
    {% for gap_hours in var('episode_gap_hours') %}
        select cast({{ gap_hours }} as int) as episode_gap_hours
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

occurrences as (
    select *
    from {{ ref('int_dontgo_ocorrencias') }}
),

expanded as (
    select
        occurrences.*,
        episode_gap_config.episode_gap_hours,
        lag(occurrences.event_time) over (
            partition by occurrences.tag, episode_gap_config.episode_gap_hours
            order by occurrences.event_time, occurrences.ocorrencia_id, occurrences.row_hash
        ) as previous_occurrence_time
    from occurrences
    cross join episode_gap_config
),

with_gaps as (
    select
        *,
        timestampdiff(
            MICROSECOND,
            previous_occurrence_time,
            event_time
        ) as gap_from_previous_microseconds
    from expanded
),

with_boundaries as (
    select
        *,
        cast(
            previous_occurrence_time is null
            or gap_from_previous_microseconds
                > cast(episode_gap_hours as bigint) * 3600000000
            as boolean
        ) as is_episode_anchor
    from with_gaps
),

with_sequences as (
    select
        *,
        sum(cast(is_episode_anchor as int)) over (
            partition by tag, episode_gap_hours
            order by event_time, ocorrencia_id, row_hash
            rows between unbounded preceding and current row
        ) as episode_sequence
    from with_boundaries
),

identified as (
    select
        *,
        sha2(
            to_json(named_struct(
                'contract_version', '{{ var("contract_version") }}',
                'episode_gap_hours', episode_gap_hours,
                'tag', tag,
                'episode_sequence', episode_sequence
            )),
            256
        ) as episode_id
    from with_sequences
),

with_episode_anchor as (
    select
        *,
        min(event_time) over (
            partition by episode_id
        ) as episode_anchor_time
    from identified
),

classified as (
    select
        *,
        cast(
            episode_anchor_time
                < timestampadd(HOUR, episode_gap_hours, dataset_start_time)
            as boolean
        ) as episode_is_left_censored
    from with_episode_anchor
)

select
    sha2(
        to_json(named_struct(
            'episode_id', episode_id,
            'ocorrencia_id', ocorrencia_id
        )),
        256
    ) as episodio_membro_id,
    '{{ var("contract_version") }}' as contract_version,
    episode_gap_hours,
    cast(episode_gap_hours = {{ var('primary_episode_gap_hours') }} as boolean) as is_primary_gap,
    episode_id,
    episode_sequence,
    episode_anchor_time,
    episode_is_left_censored,
    cast(is_episode_anchor and not episode_is_left_censored as boolean) as is_observed_episode_start,
    is_episode_anchor,
    ocorrencia_id,
    ocorrencia_sequence,
    event_time,
    previous_occurrence_time,
    gap_from_previous_microseconds,
    availability_time,
    tag,
    tag_frota,
    tipo_equipamento,
    alarme_id,
    alarme,
    criticidade_id,
    criticidade,
    row_hash,
    dataset_start_time,
    dataset_end_time
from classified
