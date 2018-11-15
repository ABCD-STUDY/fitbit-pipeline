<?php
/**
 *
 * Data store end-point for Fitbit application.
 *
 * This end-point responds to two requests, a
 * 'test' and a 'store' request. On receiving 'action=test' the end-point will
 * respond with a short JSON message:
 *   { "error": 0, "message": "ok" }
 * on 'action=send' the endpoint also expects one or more files (upload).
 *
 * Usage:
 *
 * Login:
 *   curl -F "action=test" https://abcd-report.ucsd.edu/applications/fitbit/receiver.php
 *   Result: Error HTML "Unauthorized"
 *
 *   curl --user <user name> -F "action=test" https://abcd-report.ucsd.edu/applications/fitbit/receiver.php
 *   Result: asks for password for the given user, responds with  { "message": "ok" }
 *
 * Store files:
 *   curl --user <user name> -F "action=store" https://abcd-report.ucsd.edu/applications/fitbit/receiver.php
 *   Result: Error json message: {"message":"Error: no files attached to upload"}
 *
 *   echo "1,2,3,4" > test.csv
 *   curl --user <user name> -F "action=store" -F "upload=@test.csv" https://abcd-report.ucsd.edu/applications/fitbit/receiver.php
 *   Result: A single file is stored, json object with error=0 returned
 *  
 *   echo "1,2,3,4,5" > test2.csv
 *   curl --user <user name> -F "action=store" -F "upload[]=@test.csv" -F "upload[]=@test2.csv" https://abcd-report.ucsd.edu/applications/fitbit/receiver.php
 *   Result: Two files are stored on the server, json object with error=0 returned
 *
 *
 * Example Setup:
 *   Copy this script into a directory such as /var/www/html accessible on the web-server. Create a separate
 *   directory for each site /var/www/html/d/sA and add a link there pointing to the php file.
 *   In order to secure the connection enable https and install a valid certificate (create one
 *   using let's encrypt).
 *   Add the following setting to the apache configuration file for each site (adjust site name):
 *    		<Directory /var/www/html/applications/fitbit/>
 *		  AuthType Basic
 *		  AuthName intranet
 *		  AuthUserFile /var/www/passwords
 *		  AuthGroupFile /var/www/groups
 *		  Require group siteA
 *		  Order allow,deny
 *		  Satisfy any
 *		</Directory>
 *   Use 'htpasswd' to create a password file entry for each user. Add a group
 *   file and add users to the group for specific sites. Using this type of
 *   setup each user has only access to his/her site's sub-directory and data
 *   transfer uses a secure https connection.
 *
 */

$rootdir = "/var/www/html/applications/fitbit";

log_msg("Info: called receiver.php");

if (!isset($_SERVER['PHP_AUTH_USER'])) {
    repError("Error: no user logged in");
    return;
}

$action = 'test'; // 'test' or 'store'
$site = explode("/", getcwd());
if (count($site) > 2) {
    $site = $site[count($site)-1];
} else {
    return;
}

if (isset($_POST['action'])) {
    $action = $_POST['action'];
}

$party = $_SERVER['REMOTE_ADDR'];
if (strlen($party) > 0) {
    // sanitize the name before using it
    $party = mb_ereg_replace("([^\w\s\d\-_~,;:\[\]\(\).])", '', $party);
    $party = mb_ereg_replace("([\.]{2,})", '', $party);
}

function repError( $msg ) {
    echo (json_encode( array( "error" => 1, "message" => $msg ), JSON_PRETTY_PRINT ) );
    log_msg("Error: " . $msg );
    return;
}
function repOk( $msg ) {
    echo (json_encode( array( "error" => 0, "message" => $msg ), JSON_PRETTY_PRINT ) );
    log_msg("Ok: " . $msg );
    return;
}

function log_msg( $msg ) {
    global $rootdir;
    $logFile = $rootdir.'/receiver_log.log';
    if (!is_file($logFile)) {
        file_put_contents($logFile, date(DATE_ATOM).": created file\n");
        if (!is_writable($logFile))
            return;
    }
    file_put_contents($logFile, date(DATE_ATOM).": ".$msg."\n", FILE_APPEND);
}

//
//  Call plugins that might be on the system (/plugins/<store|test>/001_some.plugin).
//  'site' is the directory name underneath '/d/<directory_name>/'
//  'filename' is the filename that needs to be run
//  'event' is either 'store' or 'test', plugin directories has different plugins for each event type
//  Each plugin will be called with the following arguments:
//      -s <site> -i <filename>
//
function callPlugins($site, $filename, $event) {
    global $rootdir;


    //$list = glob($rootdir."/plugins/".$event."/*.plugin");
    $list = glob($rootdir."/plugins/".$event."/*.php");
    $list = array_filter($list, function($val) { return !is_dir($val); });
    sort($list);
    log_msg("call active plugins (".$event."): ".json_encode($list));
    
    if (count($list) == 0) {
        return; // early exit if there are no plugins defined
    }
    
    foreach ($list as $plugin) {

        $command = $plugin . " -s \"" . $site . "\" -i \"" . $filename . "\"";
        exec($command, $output, $ok);

        if ($ok === 0) {
            log_msg("Success: could run plugin ".$plugin." on file: ".$filename);
        } else {
            log_msg("Error: plugin ".$plugin." returned error on file: ".$filename);
        }
    }
}

if ($action == 'test') {
    repError("ok");
    log_msg("test ok");
    try {
        callPlugins( $site, "", 'test' );
    } catch (Exception $e) {
        log_msg("PHP error in callPlugins");
    }
} elseif ($action == 'store') {
    log_msg("store called");
    if ($_FILES['upload']) {
        $uploads_dir = $rootdir.'/d/'.$site;
        if (!is_dir($uploads_dir)) {
            if (!mkdir($uploads_dir, 0777, true)) {
                repError( "Error: Failed to create site directory for storage" );
            }
        }
        // either we get a single file uploaded or a whole lot of them
        if (is_array($_FILES["upload"]["error"])) {
            $count = 0;
            foreach ($_FILES["upload"]["error"] as $key => $error) {
                if ($error == UPLOAD_ERR_OK) {
                    $tmp_name = $_FILES["upload"]["tmp_name"][$key];
                    # sanitize the name here
                    $name = $_FILES["upload"]["name"][$key];
                    // sanitize the name before using it
                    $name = mb_ereg_replace("([^\w\s\d\-_~,;:\[\]\(\).])", '', $name);
                    $name = mb_ereg_replace("([\.]{2,})", '', $name);
                    $name = $name."_".$party."_".date(DATE_ATOM);

                    $ok = move_uploaded_file($tmp_name, $uploads_dir."/".$name);
                    if ($ok) {
                        $count = $count + 1;
                        log_msg("uploaded file: ". $uploads_dir."/".$name);
                        try {
                            callPlugins( $site, $uploads_dir."/".$name, 'store' );
                        } catch (Exception $e) {
                            log_msg("PHP error in callPlugins");
                        }
                  
                    } else {
                        repError( "Error: failed storing file $uploads_dir/$name" );
                    }
                } else {
                    repError( "Error: upload error" );
                }
            }
            if ($count > 0) {
                repOk("Info: ".$count." file".($count > 1?"s":"")." stored" );
            } else {
                repError("Error: no file was stored." ); // there is such a thing as too much error checking
            }
        } else { // if we only get a single file uploaded
            if ( $_FILES["upload"]["error"] == UPLOAD_ERR_OK ) {
                $tmp_name = $_FILES["upload"]["tmp_name"]; 
                $name = $_FILES["upload"]["name"];
                // sanitize the name before using it
                $name = mb_ereg_replace("([^\w\s\d\-_~,;:\[\]\(\).])", '', $name);
                $name = mb_ereg_replace("([\.]{2,})", '', $name);
                $name = $name."_".$party."_".date(DATE_ATOM);
                $ok = move_uploaded_file($tmp_name, $uploads_dir."/".$name);
                if ($ok) {
                    repOk( "Info: file stored" );
                    log_msg("uploaded file: ". $uploads_dir."/".$name);
                    try {
                        callPlugins( $site, $uploads_dir."/".$name, 'store' );
                    } catch (Exception $e) {
                        log_msg("PHP error in callPlugins");
                    }
                } else {
                    repError( "Error: failed storing file $uploads_dir/$name" );
                }
            } else {
                repError( "Error: upload error" );
            }
        }
    } else {
        repError( "Error: no files attached to upload" );
    }
} else {
    repError( "Error: unknown action" );
}

?>