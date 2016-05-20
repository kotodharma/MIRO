##   READ ME CAREFULLY BEFORE EDITING
## This file is git revision-controlled in a repo named .git_dbcreds in this directory.
## In order to edit the present file, rename or symlink this repo dir to ".git", do the edits,
## check in, and THEN RENAME .git BACK TO .git_dbcreds !! (or delete the symlink)
## If you don't do this, the repo will interfere with the other stuff under MIRO/, which is
## managed by the parent directory repo. This repo IS NOT TO BE synced up to BitBucket or any
## other place outside of our direct control!

## In this hash, the key is a service name (passed in to connect method) and each value is a list
## containing [connection_string, user, password], where password or both user and password may be
## omitted for security reasons. In this case they will be prompted for on STDERR.
%Creds = (
  odsprd => ['dbi:Oracle:host=***.hawaii.edu;port=1521;service_name=ods8.db.uh', 'user'],
  odstst => ['dbi:Oracle:host=***.hawaii.edu;port=1521;service_name=odstst.db.uh', 'user'],
  odsdev => ['dbi:Oracle:host=***.hawaii.edu;port=1521;service_name=odsdev.db.uh', 'user'],
    hrdw => ['dbi:Oracle:host=127.0.0.1;port=9004;sid=uhhrdw', 'user'],
  miroprd => ['dbi:mysql:host=***.hawaii.edu;dbname=miro', 'user'],
  mirotst => ['dbi:mysql:host=***.hawaii.edu;dbname=miro_test', 'user'],
  miroprd_tun => ['dbi:mysql:host=127.0.0.1;port=19006;dbname=miro', 'user'],
  mirotst_tun => ['dbi:mysql:host=127.0.0.1;port=19006;dbname=miro_test', 'user'],
);

1;
__END__
