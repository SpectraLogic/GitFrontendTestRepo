-- Add StorageDomainMember.autoCompactionThresholdInBytes --
alter type tape.tape_type add value if not exists 'LTO5';
alter type tape.tape_type add value if not exists 'LTO6';
alter type tape.tape_type add value if not exists 'LTO7';
alter type tape.tape_type add value if not exists 'LTOM8';
alter type tape.tape_type add value if not exists 'LTO8';
alter type tape.tape_type add value if not exists 'TS_JC';
alter type tape.tape_type add value if not exists 'TS_JY';
alter type tape.tape_type add value if not exists 'TS_JK';
alter type tape.tape_type add value if not exists 'TS_JD';
alter type tape.tape_type add value if not exists 'TS_JZ';
alter type tape.tape_type add value if not exists 'TS_JL';
alter table ds3.storage_domain_member add column auto_compaction_threshold int;
update ds3.storage_domain_member set auto_compaction_threshold=(95);
