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



sub Transaction($$$)
{
    my ( $transactions_count, $sleep_time, $isolation_level ) = @_;

    my $select_query = "select count(*) from foo";
    my $update_query ="update foo set bar=? where id = ?";

    my ( $failed, $succeed, $retries )  = (0) x 3;

    my $dbh = DBI->connect("dbi:Pg:dbname=test_db;host=localhost;port=5432","dimitar","par0la",
        {
            AutoCommit=>0,
            RaiseError=>1,
            PrintError=>0
        });


    until( $succeed )
    {
        try
        {
            my $sth = $dbh->prepare("set transaction isolation level $isolation_level");
            $sth->execute();
            sleep( $sleep_time );
            $sth = $dbh->prepare( $select_query );
            $sth->execute();
            sleep( $sleep_time );
            $sth = $dbh->prepare( $update_query );
            $sth->execute("D", int(rand( $transactions_count )) + 1 );
            sleep( $sleep_time );
            $sth = $dbh->prepare( $select_query );
            $sth->execute();
            sleep( $sleep_time );
            $dbh->commit();
            $succeed = 1;
        }
        catch
        {
            $dbh->rollback();
            $failed = 1;
            $retries += 1;
        };
    }

    $dbh->disconnect();

    return ( $failed, $retries );
}


sub Handler($$$)
{
    my ( $transactions_count, $sleep_time, $isolation_level ) = @_;

    my ( $failed_transactions_count, $max_retries ) = (0) x 2;

    my $pm = new Parallel::ForkManager($transactions_count);

    $pm->run_on_finish(sub{
            my ($pid,$exit_code,$ident,$exit_signal,$core_dump,$data) = @_;

            defined( $$data{failed} ) or die "Undefined failed transactions count in proccess with pid: $pid $!";
            defined( $$data{retries} ) or die "Undefined retries in proccess with pid: $pid $!";

            $failed_transactions_count += $$data{failed};
            $max_retries = $$data{retries} > $max_retries ? $$data{retries} : $max_retries;
        });

    for ( my $forks=0; $forks < $transactions_count; $forks++ )
    {
        $pm->start and next;
        my ( $failed, $retries ) = Transaction( $transactions_count, $sleep_time, $isolation_level );
        $pm->finish(0, { failed => $failed, retries => $retries });
    }

    $pm->wait_all_children;

    $transactions_count += $failed_transactions_count;

    return ( $failed_transactions_count/( $transactions_count/100 ), $max_retries );
}

sub Main
{
    my @isolation_levels = ( "serializable", "repeatable read", "read committed" );
    my @transactions_counts = ( 10,100,1000 );
    my @sleep_times = ( 0, 0.05 );

    try
    {
        for my $isolation_level  ( @isolation_levels )
        {
            for my $transactions_count ( @transactions_counts )
            {
                PopulateTable( $transactions_count );

                for my $sleep_time ( @sleep_times )
                {
                    my ( $failed_transactions, $max_retries ) = Handler( $transactions_count, $sleep_time, $isolation_level );
                    printf "%s\t%d\t%.2f\t%.2f\t%d\n", $isolation_level, $transactions_count, $sleep_time, $failed_transactions, $max_retries;
                }
            }
        }
    }
    catch
    {
        warn "Error: $_";
    };
}


Main();
