#!/usr/bin/perl -w

use strict;
use warnings;
use Try::Tiny;
use Parallel::ForkManager;
use DBI;


sub PopulateTable($)
{
    my ($inserted_row_count) = @_;

    my @letters = ("a".."z", "A" .. "Z");
    my $truncate_query = "truncate foo";
    my $insert_query = "insert into foo ( id, bar ) values ( ?, ? )";

    my $dbh = DBI->connect("dbi:Pg:dbname=test_db;host=localhost;port=5432","dimitar","par0la",
        {
            AutoCommit=>0,
            RaiseError=>1,
            PrintError=>0
        });

    try
    {

        my $sth = $dbh->prepare( $truncate_query );
        $sth->execute();

        $sth = $dbh->prepare( $insert_query );
        for ( 1 .. $inserted_row_count )
        {
            $sth->execute($_, $letters[rand(scalar @letters)]);
        }

        $dbh->commit();
    }
    catch
    {
        $dbh->rollback();
        warn "Error: $_";
    };

    $dbh->disconnect();
}



sub Transaction($$)
{
    my ( $rows_count, $isolation_level ) = @_;

    my $select_query = "select count(*) from foo";
    my $update_query ="update foo set bar=? where id = ?";

    my $transactions_count = 0;
    my $retries_count = 0;


    my $dbh = DBI->connect("dbi:Pg:dbname=test_db;host=localhost;port=5432","dimitar","par0la",
        {
            AutoCommit=>0,
            RaiseError=>1,
            PrintError=>0
        });

    my $curr_time = time();

    while( time() < $curr_time + 5 )
    {
        try
        {
            $transactions_count += 1;
            my $id = int( rand( $rows_count ) ) + 1;
            my $sth = $dbh->prepare("set transaction isolation level $isolation_level");
            $sth->execute();
            #$sth = $dbh->prepare( $select_query );
            #$sth->execute();
            $sth = $dbh->prepare( $update_query );
            $sth->execute("D", $id);
            #$sth = $dbh->prepare( $select_query );
            #$sth->execute();
            $dbh->commit();
        }
        catch
        {
            $dbh->rollback();
            $retries_count += 1;
        };
    }

    $dbh->disconnect();

    return ( $transactions_count, $transactions_count - $retries_count, $retries_count );


}


sub Handler($$)
{
    my ( $rows_count, $isolation_level ) = @_;

    my $transactions_count = 0;
    my $retries_count = 0;
    my $succeed_transactions_count = 0;

    my $pm = new Parallel::ForkManager($rows_count);

    $pm->run_on_finish(sub{
            my ($pid,$exit_code,$ident,$exit_signal,$core_dump,$data) = @_;

            $transactions_count += $$data{transactions_count};
            $succeed_transactions_count += $$data{succeed_transactions_count};
            $retries_count += $$data{retries_count};
        });

    for ( my $forks=0; $forks < $rows_count; $forks++ )
    {
        $pm->start and next;
        my ( $transactions_count, $succeed_transactions_count, $retries_count ) = Transaction( $rows_count, $isolation_level );
        $pm->finish(0, {
                transactions_count => $transactions_count,
                succeed_transactions_count => $succeed_transactions_count,
                retries_count => $retries_count
            });
    }

    $pm->wait_all_children;

    return ( $transactions_count, $succeed_transactions_count, $retries_count );
}

sub Main
{
    my @isolation_levels = ( "serializable", "repeatable read", "read committed" );

    try
    {
        PopulateTable( 10 );
        for ( @isolation_levels )
        {

            my ( $transactions_count, $succeed_transactions_count, $retries_count ) = Handler( 10, $_ );
            printf "%s\t%d\t%d\t%d\n", $_, $transactions_count, $succeed_transactions_count, $retries_count;

        }
    }
    catch
    {
        warn "Error: $_";
    };
}


Main();
