create table if not exists public.crm_contacts (
  occurrence_id text primary key,
  source_enriched_json text not null,
  target_url text,
  hotel_name text not null,
  full_name text not null,
  title text,
  primary_handle text,
  phone text,
  phone2 text,
  email text,
  email2 text,
  linkedin_url text,
  x_handle text,
  other_contact_detail text,
  decision_maker_score text,
  intimacy_grade text,
  has_phone boolean not null default false,
  has_email boolean not null default false,
  has_contact_route boolean not null default false,
  status text not null default 'pending' check (status in ('pending', 'done', 'skipped')),
  notes text not null default '',
  payload jsonb not null,
  source_hash text not null,
  source_synced_at timestamptz not null default now(),
  crm_updated_at timestamptz not null default now()
);

create index if not exists crm_contacts_hotel_pending_idx
  on public.crm_contacts (hotel_name, status, has_contact_route);

create index if not exists crm_contacts_phone_idx
  on public.crm_contacts (has_phone);

create index if not exists crm_contacts_status_idx
  on public.crm_contacts (status);

create index if not exists crm_contacts_name_idx
  on public.crm_contacts (lower(full_name));

alter table public.crm_contacts enable row level security;

create or replace function public.crm_touch_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.crm_updated_at = now();
  return new;
end;
$$;

drop trigger if exists crm_contacts_touch_updated_at on public.crm_contacts;
create trigger crm_contacts_touch_updated_at
before update of notes, status on public.crm_contacts
for each row
execute function public.crm_touch_updated_at();

create or replace function public.crm_upsert_contacts(rows jsonb)
returns integer
language plpgsql
as $$
declare
  inserted_count integer;
begin
  with input_rows as (
    select *
    from jsonb_to_recordset(rows) as x(
      occurrence_id text,
      source_enriched_json text,
      target_url text,
      hotel_name text,
      full_name text,
      title text,
      primary_handle text,
      phone text,
      phone2 text,
      email text,
      email2 text,
      linkedin_url text,
      x_handle text,
      other_contact_detail text,
      decision_maker_score text,
      intimacy_grade text,
      has_phone boolean,
      has_email boolean,
      has_contact_route boolean,
      payload jsonb,
      source_hash text
    )
  ),
  upserted as (
    insert into public.crm_contacts (
      occurrence_id,
      source_enriched_json,
      target_url,
      hotel_name,
      full_name,
      title,
      primary_handle,
      phone,
      phone2,
      email,
      email2,
      linkedin_url,
      x_handle,
      other_contact_detail,
      decision_maker_score,
      intimacy_grade,
      has_phone,
      has_email,
      has_contact_route,
      payload,
      source_hash,
      source_synced_at
    )
    select
      occurrence_id,
      source_enriched_json,
      target_url,
      hotel_name,
      full_name,
      title,
      primary_handle,
      phone,
      phone2,
      email,
      email2,
      linkedin_url,
      x_handle,
      other_contact_detail,
      decision_maker_score,
      intimacy_grade,
      coalesce(has_phone, false),
      coalesce(has_email, false),
      coalesce(has_contact_route, false),
      payload,
      source_hash,
      now()
    from input_rows
    on conflict (occurrence_id) do update set
      source_enriched_json = excluded.source_enriched_json,
      target_url = excluded.target_url,
      hotel_name = excluded.hotel_name,
      full_name = excluded.full_name,
      title = excluded.title,
      primary_handle = excluded.primary_handle,
      phone = excluded.phone,
      phone2 = excluded.phone2,
      email = excluded.email,
      email2 = excluded.email2,
      linkedin_url = excluded.linkedin_url,
      x_handle = excluded.x_handle,
      other_contact_detail = excluded.other_contact_detail,
      decision_maker_score = excluded.decision_maker_score,
      intimacy_grade = excluded.intimacy_grade,
      has_phone = excluded.has_phone,
      has_email = excluded.has_email,
      has_contact_route = excluded.has_contact_route,
      payload = excluded.payload,
      source_hash = excluded.source_hash,
      source_synced_at = now()
    returning 1
  )
  select count(*) into inserted_count from upserted;

  return inserted_count;
end;
$$;

