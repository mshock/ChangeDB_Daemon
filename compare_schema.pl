#!perl -w

# compare the column schema for all matching tables between two databases
# TODO: possible bug w/ importing seeds with more initial rows than int type can enumerate (convert all RowNum_ keys to bigint, default row_number() output)
# TODO: auto-fix table/schema mismatch
# TODO: write + use BCPlib to handle BCP operations

use strict;
use Config::Simple;
use Getopt::Std;
use Time::Duration qw(from_now);
use POSIX qw(strftime);
use DBI;

# load CLI options
my %cli_opts;
getopts('c:dfhi:l:stv', \%cli_opts);

# only load Date::Parse if -d switch is enabled
if ($cli_opts{d}) {
	require Date::Parse;
	Date::Parse->import();
}

# print usage if -h or unprocessed CLI args
usage() if $cli_opts{h} || $ARGV[0];

# toggles STDOUT status updates
my $verbose = $cli_opts{v} || $cli_opts{t};

# regex global for affirmative
my $regex_true = qr/true|y|yes|1/i;

# load the configs from file
my $cfg = load_cfg();
my ($db1,$db2,$db3) = ($cfg->param(-block=>"db1"),$cfg->param(-block=>"db2"), $cfg->param(-block=>"db3"));
my ($db1_name, $db2_name) = ($db1->{name},$db2->{name}); 
my @db_names = ($db1->{name},$db2->{name});
my $conf_opts = $cfg->param(-block=>"opts");
# load column exclusions
my %excludes = map {$_ => 1} split ',', uc $conf_opts->{excludes};
# load verbosity from config file, CLI switch overrides
$verbose = $conf_opts->{verbose} =~ $regex_true if $conf_opts->{verbose} && !$verbose;

# back-check tables from database2 against database1
my $table_backcheck = $cli_opts{b} || $conf_opts->{back_check} =~ $regex_true;

# set downtime
my $allow_table_copy = $conf_opts->{enable_table_copy} =~ $regex_true;
my $downtime = $conf_opts->{downtime} || '12:00pm next friday';

# check if this is a run to generate a default ignore.conf file
my $gen_ignore = $conf_opts->{generate_ignore} =~ $regex_true;
# clear the ignore file if generating a new one
(open(TMP, '>', $cli_opts{i} || 'ignore.conf')
	and close TMP) if $gen_ignore;
# load ignore rules from config file
my $ignore_cfgs =  !$gen_ignore ? load_ignore($cli_opts{i} || 'ignore.conf') : {};

vprint("configs loaded successfully\n");

# auto-fix verify intent
verify_continue('
	<<auto-fix mode enabled>>
this will potentially drop/alter tables
are you sure you want to proceed?') if $cli_opts{f};

# begin writing to $log_handle file
my ($log_handle, $log_format) = handle_log();

# create database handles using configs
vprint("intializing database handles....");
my @dbhs = init_handles($db1, $db2);
vprint("ok\n");

# get all table names from both databases
vprint("fetching tables...");
my $names_query = 'select UPPER(name) from sys.tables';
my @db_tables_q = map {$_->prepare($names_query)} @dbhs;
map {$_->execute()} @db_tables_q;

my @db_tables = map {$_->fetchall_hashref(1)} @db_tables_q;
map {$_->finish()} @db_tables_q;
vprint("ok\n");

# prepare queries to compare schema
vprint("preparing schema queries...");
my $schema_query = '
	select UPPER(c.name),UPPER(t.name) as type 
	from sys.columns c 
	join sys.types t 
	on t.system_type_id = c.system_type_id 
	where object_id = OBJECT_ID(CAST(? as varchar))';
my @db_schema_q = map {$_->prepare($schema_query)} @dbhs;
vprint("ok\n");

# optionally initialize vars for table update comparison
my ($db1_crdate,@db_lastupd_q) = 
	$cli_opts{d} || $conf_opts->{check_recent} =~ $regex_true ? init_tableupd() : (undef, undef);

# config test CLI option stops exec here
config_test() if $cli_opts{t};

vprint("running comparison between $db1_name & $db2_name...\n");
# write config header to ignore configs if generating ignore file
write_ignore('header','tables') if $gen_ignore;

# compare table schema from db1 against db2
for my $table (sort keys %{$db_tables[1]}) {
	# don't care about changes tables
	next if $table =~ /_CHANGES/;
	# skip ignored tables
	my $next_flag;
	for (@{$ignore_cfgs->{tables}}) {
		$next_flag++ if $table =~ /^$_$/i;
	}
	next if $next_flag;
	
	if (!$db_tables[0]->{$table}) {
		my $err_msg = "$table not found on $db1_name";
		write_log('[TABLE EXISTANCE]',$err_msg,$table);
		#printf $log_handle $log_format, '[TABLE EXISTANCE]', $err_msg;
		if ($allow_table_copy) {
			copy_table($table);
			$err_msg .= ', scheduling copy';
		}
		vprint("\t\t$err_msg\n");
		next;
	}
}

# header for optional ignore file gen
write_ignore('header','columns') if $gen_ignore;

# compare table schema from db2 against db1
for my $table (sort keys %{$db_tables[0]}) {
	# skip ignored tables
	my $next_flag;
	for (@{$ignore_cfgs->{tables}}) {
		#print "comparing $table against $_\n";
		$next_flag++ if $table =~ /^$_$/i;
	}
	next if $next_flag;
	
	vprint("\tverifying schema for $table...\n");
	# only check existence of tables from db1 to db2 if explicitly asked, usually don't care
	if ($table_backcheck && !$db_tables[1]->{$table}) {
		my $err_msg = "$table not found on $db2_name";
		printf $log_handle $log_format, '[TABLE EXISTANCE]', $err_msg;
		vprint("\t\t$err_msg\n");
		next;	
	}

	# optionally compare update times for tables
	if ($cli_opts{d}) {
		if (my $msg = cmp_tableupd($table)) {
			printf $log_handle $log_format, '[  TABLE SYNC   ]', $msg;
			vprint("\t\t\t$msg\n");
		}
	}
	
	map {$_->execute($table)} @db_schema_q;
	my @db_schema = map {$_->fetchall_hashref(1)} @db_schema_q;
	map {$_->finish()} @db_schema_q;
	

	for my $column (sort keys %{$db_schema[1]}) {
		# skip if in exclusions
		next if $excludes{$column};
		
		my $next_flag;
		for (@{$ignore_cfgs->{columns}}) {
			my ($cur_table, $cur_col) = split "\t";
			$next_flag++ if $column =~ /^$cur_col$/i && $table =~ $cur_table;
		}
		next if $next_flag;
		
		# check if column is in both databases
		if (!$db_schema[0]->{$column}) {
			my $err_msg = "$table => $column column not found on $db1_name";
			write_log('[ COL EXISTANCE ]', $err_msg, $table, $column);
			#printf $log_handle $log_format, '[ COL EXISTANCE ]', $err_msg;
			vprint("\t\t$err_msg\n");
			next;
		}
		# check if datatype is the same for both database columns
		my @db_type = map {$_->{$column}->{'type'}} @db_schema;
		if($db_type[0] ne $db_type[1]) {
			my $err_msg = "$table => $column => $db_type[0] ($db1_name) vs $db_type[1] ($db2_name)";
			write_log('[ COL DATATYPE  ]', $err_msg, $table, $column);
			#printf $log_handle $log_format, '[ COL DATATYPE  ]', $err_msg;
			vprint("\t\t$err_msg\n");
			next;
		}
	}	
}
cleanup();
vprint("\nfinished\n");


##################################################
#	subs
#
##################################################

sub write_log {
	my ($type, $err_msg, $table, $column) = @_;
	# print standard message to log file
	printf $log_handle $log_format, $type, $err_msg
		or warn "could not write to log file: ";
	# write ignore config file if enabled
	write_ignore($type, $err_msg, $table, $column) if $gen_ignore;
	return 1;
}

sub write_ignore {
	my ($type, $err_msg, $table, $column) = @_;
	# sub appends to ignore config
	# double check that option is enabled
	return unless $gen_ignore;
	my $rval = 1;
	open (my $ignore_handle, '>>',  $cli_opts{i} || 'ignore.conf');
	if ($type =~ /table existance/i) {
		print $ignore_handle "$table\n";
	}
	elsif ($type =~ /col [existance|datatype]/i) {
		print $ignore_handle "$table\t$column\n";
	}
	elsif ($type =~ /header/) {
		print $ignore_handle "\n[$err_msg]\n";
	}
	else {
		warn "unsupported log entry type: $type\n$err_msg\n";
		$rval = 0;
	}
	close $ignore_handle;
	return $rval;
}

sub load_cfg {
	vprint("loading configs from file...");
	my $cfg_file = shift;
	$cfg_file ||= $cli_opts{c} ||  'compare_schema.conf';
	die "Config file not found: $cfg_file\n" if ! -f $cfg_file;
	my $cfg = new Config::Simple($cfg_file);
	vprint("ok\n");
	return $cfg;
}

# handle finding/creating log file
sub handle_log {
	my $log_file = $cli_opts{l} || $conf_opts->{log_file} || "${db1_name}_vs_$db2_name.log";
	vprint("initializing $log_file...");
	my $log_format = "%-25s%s\n";
	open(my $log_handle, '>>', $log_file) or die "could not open $log_file: $log_file\n";
	my $header_format = "%-25s\t%s | %s\n";
	$header_format = "\n" . $header_format if tell($log_handle);
	printf $log_handle $header_format, strftime('[%c GMT]',gmtime), $db1_name, $db2_name;
	vprint("ok\n") if !$cli_opts{t};
	return ($log_handle, $log_format);
}


# print updates to STDOUT if verbose enabled
# write STDOUT messages to $log_handle for config test
sub vprint {
	my ($msg) = @_;
	print $msg if $verbose;
	print $log_handle $msg if $cli_opts{t} && tell($log_handle) >= 0;
}

# end of configuration test (-t)
# add additional tests here
sub config_test {
	print $log_handle "config test run finished\n";
	cleanup();
	print "configuration test complete\n";
	exit(0);
}

# create database handles
sub init_handles {
	my @db_info = @_;
	
	my @dbhs;
	for my $db (@db_info) {
		my $dbh = DBI->connect(
			sprintf("dbi:ODBC:Driver={SQL Server};Database=%s;Server=%s;UID=%s;PWD=%s",
				$db->{name},
				$db->{server},
				$db->{user},
				$db->{pwd}
			)	
		) or die DBI->errstr;
		push @dbhs, $dbh;
	}
	return @dbhs;
}

# prepare queries for comparing most recent table updates
sub init_tableupd {
	
	# get the creation datetime of db1 for table update staleness cmps
	vprint("retrieving $db1_name creation datetime...");
	my $db_crdate_query = "
		select crdate
		from sys.sysdatabases
		where name = CAST(? as varchar)";
	my $db1_crdate_q = $dbhs[0]->prepare($db_crdate_query);
	$db1_crdate_q->execute($db1_name);
	my $db1_crdate = ($db1_crdate_q->fetchrow_array())[0];
	$db1_crdate_q->finish();
	vprint("ok\n");
	
	# prepare queries for getting most recent table updates
	vprint("preparing update queries for most recent table updates...");
	my $lastupd_query = "
		SELECT top 1 max(starttime) as starttime, filedate, filenum
  		FROM [update_log]
  		where tablename = CAST(? as varchar)
		group by filedate, filenum
		order by filedate desc";
	my @db_lastupd_q = map {$_->prepare($lastupd_query)} @dbhs;
	vprint("ok\n");

	return ($db1_crdate,@db_lastupd_q);
}

# compare table update times to verify sync
sub cmp_tableupd {
	my ($table) = @_;
	
	my $msg;
	
	# maximum time to allow between table updates from config file (seconds)
	my $max_updtime = $conf_opts->{update_diff};
	
	map {$_->execute($table)} @db_lastupd_q;
	my @db_lastupd = map {$_->fetchrow_arrayref()} @db_lastupd_q;
	map {$_->finish()} @db_lastupd_q;
	
	#print "$db_lastupd[0]->[0]\n$db_lastupd[1]->[0]\n$db1_crdate";
	#exit;
	
	
	# get epoch times from strings
	my @epoch_upds = map {str2time($_)} map({$_->[0]}@db_lastupd), $db1_crdate;
	
	#print "$epoch_upds[0] $epoch_upds[1]\n";
	#exit;
	
	# either/both times are null... no updates ever
	if (!$epoch_upds[0] || !$epoch_upds[1]) {
		$msg .= "$table ";
		# db1 nor db2 have any updates - table probably defunct/inactive
		if (!$epoch_upds[0] && !$epoch_upds[1]) {
			return 0 if !$cli_opts{s};
			$msg .= "- neither database has updated this table";
		}
		# db1 is null and there have been updates in db2 since db1 creation
		# updates might not be applying properly to db1
		elsif (!$epoch_upds[0] && ($epoch_upds[1] > $epoch_upds[2])) {
			$msg .= "on $db1_name has never been updated (possible loading bug)";
			
		}
		# db1 is getting updates, db2 has never had an update
		else {
			return 0 if !$cli_opts{s};
			$msg .= "on $db2_name has never been updated (possibly defunct)";	
		}
		
		# if both have had updates, check for UPD mismatch (sometimes normal)
		if ($epoch_upds[0] && $epoch_upds[1]) {
			$msg .= cmp_recent_upd(@db_lastupd);
		} 
		
		return $msg;
	}
	
	# if absolute difference between update times is greater than threshold
	# there is an update mismatch likely
	if ((my $out_sync = abs($epoch_upds[0] - $epoch_upds[1])) > $max_updtime) {
		
		# get UPD filenames
		my @upds = map {upd_format($_->[1],$_->[2])} @db_lastupd;
		
		my ($msg, $hr_sync) = ("$table on ", sec_hrtime($out_sync));		
		# determine which db is late (most likely db2 due to apply time)
		if ($epoch_upds[0] > $epoch_upds[1] || $upds[0] lt $upds[1]) {
			# only show db2 sync issues if -s
			return 0 if !$cli_opts{s} && $upds[0] eq $upds[1];
			$msg .= ($upds[0] lt $upds[1] ? $db1_name : $db2_name) . " is out of sync - $hr_sync";
		}
		else {
			return 0 if !$cli_opts{s};
			$msg .= "$db2_name apply time or out of sync - $hr_sync";			
		}
		
		# if both have had updates, check for UPD mismatch (sometimes normal)
		if ($epoch_upds[0] && $epoch_upds[1]) {
			$msg .= cmp_recent_upd(@db_lastupd);
		} 
		
		return $msg;
	}
	return 0;
}

# schedule a transfer of new table seed from db2 to db1
sub copy_table {
	my ($table) = @_;
	# only import (huge) Date::Manip if needed at runtime
	require Date::Manip;
	Date::Manip->import(qw(UnixDate));
	# calculate amount of time between now and scheduled 'downtime'
	my $epoch_downtime = UnixDate($downtime,'%s')
		or die "could not parse downtime: $downtime\n";
	my $sleep_duration = $epoch_downtime - time;
	
	# fork a sleeping child (phrasing)
	fork or bcp_table($table, $sleep_duration);
	return 1;
}

# child sub
# use BCP copy table seed
# TODO: write my own BCP library - CPAN's Sybase library is funky
sub bcp_table {
	my ($table, $sleep_duration) = @_;
	
	# stuff to do before going to sleep goes here
	bcp_prep($table, $sleep_duration);
	
	# sleep until scheduled to run
	sleep $sleep_duration;
	open(my $bcp_log, '>>', "Logs/$table/$table.log")
		or die "could not open bcp log @ Logs/$table/$table.log\n";
	
	my $error_path = "Logs\\$table\\error";
	my $bcp_path = "Logs\\$table\\$table.bcp";
	
	# INV: BCP format file can be used to add extra columns
	my $firstcol_query = "select column_name from information_schema.columns where table_name = '$table' and ordinal_position = 0";
	my $select_query = "select cast(0 as int) as 'FileDate_', cast(0 as int) as 'FileNum_', row_number() over (order by ($firstcol_query)) as 'RowNum_', cast('A' as char(1)) as 'UpdateFlag_',* from [$db2_name].dbo.[$table] with (NOLOCK)";
	# execute BCP export remote to local
	print $bcp_log `bcp "$select_query" queryout $bcp_path -S$db2->{server} -U$db2->{user} -P$db2->{pwd} -c -e$error_path.export_errors`;
		
	# execute BCP import local
	print $bcp_log `bcp [$db3->{name}].dbo.[$table] in $bcp_path -S$db3->{server} -U$db3->{user} -P$db3->{pwd} -c -e$error_path.import_errors`;
	
	# TODO: verify table copy somehow here + assign seed UPD filedate/filenum
	update_seed($table)
		or print $bcp_log "filedate/filenum update on seed table failed\n";
	
	# delete bcp file
	#unlink("$table.bcp") or print $bcp_log "could not delete BCP file: $table.bcp\n";
	close $bcp_log;
}

# pre-processing for table import w/ BCP
sub bcp_prep {
	my ($table, $sleep_duration) = @_;
	
	# create a directory for this table's logs and bcp file
	mkdir("Logs/$table");
	#	die "could not create table copy directory for $table\n";
	
	# write a file stating that this copy is scheduled to run
	open(my $sched_log, '>', "Logs/$table/schedule.txt");
	# make the sleep duration human readable
	my $hr_sleep_duration = from_now($sleep_duration);
	print $sched_log "The table $table is/was/will be scheduled to import at the next downtime.
downtime: $downtime
scheduled: $hr_sleep_duration
seconds: $sleep_duration";
	close $sched_log;
	
	# create the table in the seed database (ChangeDB)
	
	# get new DBI handles b/c this is a child
	@dbhs = init_handles($db3, $db2, $db1);
	
	# create table in the change database
	create_table($table, $dbhs[2], $db1_name, "Logs/$table/create_current.log")
		or die "could not create update table\n";
	
	# create table in the seed database
	create_table($table, $dbhs[0], $db3->{name}, "Logs/$table/create_seed.log")
	 	or die "could not create seed table\n";
	
}


# TODO: add option to create from BCP format file
# create a changedb table from a client table
sub create_table {
	my ($table, $dbh, $database, $log) = @_;
	
	# defaults to top level of logdir
	$log ||= 'Logs/create.log';
	
	# query that will be executed at the end of this routine to create the table
	my @queries;
	
	# open the log for this
	open(my $create_log, '>', $log)
		or die "could not open log for table create @ Logs/$table/create.log\n";
	
	# determine the filegroup of the new table
	# only as reliable as the sys tables are (not always actively updated)
	my $filegroup = ($dbhs[1]->selectrow_array("
		select fg.name as 'FILEGROUP' 
		from sys.filegroups fg
		join sys.indexes ind
		on ind.data_space_id = fg.data_space_id
		join sys.tables tab
		on tab.object_id = ind.object_id
		where tab.name = '$table'"
	))[0];
		
	# check result if table belongs to a filegroup
	unless ($filegroup) {
		print $create_log "no filegroup found for table $table\n"
			and warn "no filegroup found for table $table\n";
	}
	else {
		# different filegroup for current database
		my $fg_name = $filegroup;
		if ($database =~ /_current/i) {
			$fg_name .= '_current';
		}
		print $create_log "remote filegroup found: $filegroup\n";
		
		# check if filegroup already exists in this database
		 my $filegroup_check = ($dbh->selectrow_array("
			select top 1 fg.name as 'FILEGROUP' 
			from sys.filegroups fg
			join sys.indexes ind
			on ind.data_space_id = fg.data_space_id
			join sys.tables tab
			on tab.object_id = ind.object_id
			where fg.name = '$filegroup'"))[0];
		# if it doesn't exist, add create filegroup to the query 
		unless ($filegroup_check) {
			print $create_log "local filegroup not found, creating\n";
			my $create_query;
			# add the filegroup
			$create_query = "alter database [$database] add FILEGROUP [$filegroup]\n";
			push @queries, $create_query;
			# add the file associated w/ the filegroup
			$create_query = "alter database [$database] add FILE (
				NAME = $filegroup,
				FILENAME = 'D:\\MSSQL\\DATA\\$fg_name.ndf',
				MAXSIZE = UNLIMITED,
				FILEGROWTH = 10%
			) to FILEGROUP [$filegroup]\n";
			push @queries, $create_query;
		}
		else {		
			print $create_log "local filegroup found: $filegroup_check\n";
		}
	}
		
	# append create table statement
	my $create_query = "
		create table [$database].dbo.[$table]
			(
				FileDate_ int not null,
   				FileNum_ int  not null,
   				RowNum_ int  not null,
   				UpdateFlag_ char(1) not null,\n";
		
	# get table schema from db2
 	my $schema = $dbhs[1]->selectall_arrayref("
 		select column_name, is_nullable, data_type, character_maximum_length
 		from information_schema.columns
 		where table_name = '$table'"
 	);
	 	
	# get these columns into a table create format
	for my $col (@{$schema}) {
		unless (scalar @{$col}) {
			print $create_log "empty table column schema row found\n";
			next;
		}
		# unpack row
		my ($cname, $cnull, $ctype, $clength) = @{$col};
					
		# if the type has a length, add it parenthetically
		if ($clength) {
			$ctype .= "($clength)";
		}
		
		# if nullable, convert to string
		if ($cnull =~ /no/i) {
			$cnull = 'not null';
		}
		else {
			$cnull = 'null'
		}
		
		$create_query .= "$cname $ctype $cnull,\n";
	}
	
	$create_query .= "Constraint pkey_$table primary key Clustered (FileDate_, FileNum_, RowNum_, UpdateFlag_) on $filegroup
	)
	on $filegroup\n";
	
	push @queries, $create_query;
	
	# TODO: create indexes
	
	print $create_log "created queries:\n", join("\n", @queries);

	# execute all generated queries
	# sleep 5 seconds between each to allow filegroups to get created
	map {$dbh->do($_)} @queries; 
	#	or print $create_log "create query for table $table seems to have failed\n", $dbhs[0]->errstr;
	close $create_log;
}



# update the seed 
sub update_seed {
	my ($table) = @_;

	# determine the last UPD
	
	# call SmartLoader to apply on seed
	
	# update all UPD metadata for seed 

	return 1;
}

# compare 2 rows from update_log upd record
sub cmp_recent_upd {
	my @rows = @_;
	
	my ($fd1,$fn1,$fd2,$fn2) = map {@{$_}[1..2]} @rows;
	# (@{$rows[0]}[1..2],@{$rows[1]}[1..2]);
	
	# compare filenum
	my $fn_equal = $fn1 <=> $fn2;
	# compare filedate
	my ($ts1,$ts2) = map {str2time($_)} ($fd1, $fd2); 
	my $fd_equal =  $ts1 <=> $ts2;
	
	# convert sql datetime + filenum to YYYYMMDD-N
	my ($upd1, $upd2) = map {upd_format($_->[0],$_->[1])} ([$fd1,$fn1], [$fd2,$fn2]);
	
	# both dbs are on the same UPD
	# or db1 is ahead and -s switch not enabled
	
	# both dbs are on exactly the same upd
	return " on UPD - $upd1" if (!$fn_equal && !$fd_equal);
	# they are on different UPDs
	#return " on diff UPD - ($db1_name) $upd1 vs ($db2_name) $upd2" ((($fn1 > $fn2) && ($ts1 == $ts2) || ($ts1 > $ts2)) && !$cli_opts{s});
	return " on diff UPD - ($db1_name) $upd1 vs ($db2_name) $upd2" if ($cli_opts{s} || ($upd1 lt $upd2));
	
}

# convert number of seconds to human readable time
sub sec_hrtime {
	my ($tot_sec) = @_;
	
	my ($months, $days, $hours, $minutes, $seconds);
	my ($format, @prints);
	
	$format .= '%02dM:' and push @prints, $months if $months = int($tot_sec / 2.62974e6);
	$tot_sec -= int($months * 2.62974e6);
	$format .= '%02dd:' and push @prints, $days if ($days = int($tot_sec / 86400)) || $months;
	$tot_sec -= int($days * 86400);
	$format .= '%02dh:' and push @prints, $hours if ($hours = int($tot_sec / 3600)) || $days;
	$tot_sec -= int($hours * 3600);
	$format .= '%02dm' and push @prints, $minutes if ($minutes = int($tot_sec / 60)) || $hours;
	$tot_sec -= int($minutes * 60);
	$format .= ':%02ds' and push @prints, $tot_sec;	
	return sprintf $format, @prints;
}

# convert sql datetime to UPD filedate/filenum format
sub upd_format {
	my ($datetime, $filenum) = @_;
	
	$datetime =~ m/(\d{4})-(\d{2})-(\d{2})/ 
		or die "upd_format() failed to parse datetime $datetime\n";
		
	return sprintf '%04d%02d%02d-%02d', $1, $2, $3, $filenum;
}

# takes a message, exits/continues based on user input
sub verify_continue {
	my ($msg) = @_;
	
	print $msg . ' [y/N]: ';
	my $input = <>;
	print "\n";
	if ($input !~ $regex_true) {
		exit(0);
	}
}

# release all resources
sub cleanup {
	vprint("cleaning up...");
	map {$_->disconnect()} @dbhs;
	close $log_handle;
	vprint("ok\n");
}

# load ignored tables/columns from file
# Config::Simple ought to have a way to do simple listing...
sub load_ignore {
	my ($conf_file) = @_;
	open(my $ignore_fh, '<', $conf_file)
		or warn "could not open ignore configuration file: $conf_file\n"
		and return;
		
	my ($current_header);
	my $ignores_href = {};
	while(<$ignore_fh>) {
		chomp;
		if(/^\[(\w+)\]$/../^$/) {
			unless ($current_header) {
				$current_header = $1;
				next;	
			}			
			push @{$ignores_href->{$current_header}}, $_ if $_;
		}
		else {
			$current_header = '';
		}
	}	
	close $ignore_fh;
	return $ignores_href;
}

# standard usage message
sub usage {
	print "
usage: compare_schema.pl [-dfhstv][-c config_file][-l log_file]
	-c:	specify config file (default: compare_schema.conf)
	-d	verify modification datetimes between tables
	-f	force db1 to match db2 in schema (if table is empty) and drop unmatched tables, will prompt 
	-h 	print this usage message
	-i: specify table/column ignore config file  
	-l:	specify log file (default: {db1}_vs_{db2}.log
	-s 	show all table sync errors
	-t	test configs mode (enables verbose)
	-v	verbose mode
";

	# exit with error if help not explicitly called
	exit($cli_opts{h} ? 0 : 1);
}