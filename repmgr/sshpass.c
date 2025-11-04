/*  This file is part of "sshpass", a tool for batch running password ssh authentication
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

#if HAVE_CONFIG_H
#include "config.h"
#endif

#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/ioctl.h>
#include <sys/select.h>

#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#if HAVE_TERMIOS_H
#include <termios.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include "sshpass.h"
#include "repmgr.h"

enum program_return_codes {
	RETURN_NOERROR,
	RETURN_INVALID_ARGUMENTS,
	RETURN_CONFLICTING_ARGUMENTS,
	RETURN_RUNTIME_ERROR,
	RETURN_PARSE_ERRROR,
	RETURN_INCORRECT_PASSWORD,
	RETURN_HOST_KEY_UNKNOWN,
	RETURN_HOST_KEY_CHANGED,
	RETURN_HELP,
};

// Some systems don't define posix_openpt
#ifndef HAVE_POSIX_OPENPT
int
posix_openpt(int flags)
{
	return open("/dev/ptmx", flags);
}
#endif

void reliable_write( int fd, const void *data, size_t size );
int handleoutput( int fd , const char *password);
void window_resize_handler(int signum);
void sigchld_handler(int signum);
void term_handler(int signum);
void term_child(int signum);
int match( const char *reference, const char *buffer, ssize_t bufsize, int state );

/* Global variables so that this information be shared with the signal handler */
static int ourtty; // Our own tty
static int masterpt;

int childpid;
int termsig;

int runsshpass( const char *password, const char *ssh_options, const char *command, int pfd[2] )
{
	struct winsize  ttysize; // The size of our tty
	sigset_t        sigmask, sigmask_select;
	pid_t           wait_id = 0;
	char            options[MAXLEN] = "";
	char            command_shell[MAXLEN] = "";
	const char      *name;
	char            *strtemp;
	char            **argv;
	int             i = 0;
	int             status = 0;
	int             terminate = 0;
	int             slavept;

	/* begin add by houjiaxing for #178952 at 2023/03/16 reveiwer huyuanni. */
	strcpy(options, ssh_options);
	strcpy(command_shell, command);
	/*
	 * get ssh argv
	 */
	argv = malloc(sizeof(char *)*(9));
	memset(argv, 0, sizeof(char *)*(9));

	/* 获取第一个子字符串 */
	strtemp = strtok(options, " ");

	/* 继续获取其他的子字符串 */
	while( strtemp != NULL && i < 9 )
	{
		argv[i] = strtemp;
		strtemp = strtok(NULL, " ");
		i++;
	}
	argv[i] = command_shell;
	/* end add by houjiaxing for #178952 at 2023/03/16 reveiwer huyuanni. */

	// We need to interrupt a select with a SIGCHLD. In order to do so, we need a SIGCHLD handler
	signal( SIGCHLD, sigchld_handler );

	// Create a pseudo terminal for our process
	masterpt = posix_openpt(O_RDWR);

	if( masterpt == -1 ) {
		perror("Failed to get a pseudo terminal");

		return RETURN_RUNTIME_ERROR;
	}

	fcntl(masterpt, F_SETFL, O_NONBLOCK);

	if( grantpt( masterpt ) != 0 ) {
		perror("Failed to change pseudo terminal's permission");

		return RETURN_RUNTIME_ERROR;
	}
	if( unlockpt( masterpt ) != 0 ) {
		perror("Failed to unlock pseudo terminal");

		return RETURN_RUNTIME_ERROR;
	}

	ourtty=open("/dev/tty", 0);
	if( ourtty != -1 && ioctl( ourtty, TIOCGWINSZ, &ttysize ) == 0 ) {
		signal(SIGWINCH, window_resize_handler);

		ioctl( masterpt, TIOCSWINSZ, &ttysize );
	}

	name=ptsname(masterpt);
	/*
	   Comment no. 3.14159

	   This comment documents the history of code.

	   We need to open the slavept inside the child process, after "setsid", so that it becomes the controlling
	   TTY for the process. We do not, otherwise, need the file descriptor open. The original approach was to
	   close the fd immediately after, as it is no longer needed.

	   It turns out that (at least) the Linux kernel considers a master ptty fd that has no open slave fds
	   to be unused, and causes "select" to return with "error on fd". The subsequent read would fail, causing us
	   to go into an infinite loop. This is a bug in the kernel, as the fact that a master ptty fd has no slaves
	   is not a permenant problem. As long as processes exist that have the slave end as their controlling TTYs,
	   new slave fds can be created by opening /dev/tty, which is exactly what ssh is, in fact, doing.

	   Our attempt at solving this problem, then, was to have the child process not close its end of the slave
	   ptty fd. We do, essentially, leak this fd, but this was a small price to pay. This worked great up until
	   openssh version 5.6.

	   Openssh version 5.6 looks at all of its open file descriptors, and closes any that it does not know what
	   they are for. While entirely within its prerogative, this breaks our fix, causing sshpass to either
	   hang, or do the infinite loop again.

	   Our solution is to keep the slave end open in both parent AND child, at least until the handshake is
	   complete, at which point we no longer need to monitor the TTY anyways.
	 */

	// Set the signal mask during the select
	sigemptyset(&sigmask_select);

	// And during the regular run
	sigemptyset(&sigmask);
	sigaddset(&sigmask, SIGCHLD);
	sigaddset(&sigmask, SIGHUP);
	sigaddset(&sigmask, SIGTERM);
	sigaddset(&sigmask, SIGINT);
	sigaddset(&sigmask, SIGTSTP);

	sigprocmask( SIG_SETMASK, &sigmask, NULL );

	signal(SIGHUP, term_handler);
	signal(SIGTERM, term_handler);
	signal(SIGINT, term_handler);
	signal(SIGTSTP, term_handler);

	childpid = fork();
	if( childpid == 0 ) {
		// Child

		/* begin add by houjiaxing for #178952 at 2023/03/16 reveiwer huyuanni. */
		close(pfd[0]); 
		if (pfd[1] != STDOUT_FILENO) {  
			dup2(pfd[1], STDOUT_FILENO);  
			close(pfd[1]); 
		}
		/* end add by houjiaxing for #178952 at 2023/03/16 reveiwer huyuanni. */

		// Re-enable all signals to child
		sigprocmask( SIG_SETMASK, &sigmask_select, NULL );

		// Detach us from the current TTY
		setsid();

		// Attach the process to a controlling TTY.
		slavept = open(name, O_RDWR | O_NOCTTY);
		// On some systems, an open(2) is insufficient to set the controlling tty (see the documentation for
		// TIOCSCTTY in tty(4)).
		if (ioctl(slavept, TIOCSCTTY, 0) == -1) {
			perror("sshpass: Failed to set controlling terminal in child (TIOCSCTTY)");
			exit(RETURN_RUNTIME_ERROR);
		}
		close( slavept ); // We don't need the controlling TTY actually open

		close( masterpt );
		/* modify by houjiaxing for #178952 at 2023/03/16 reveiwer huyuanni. */
		execvp( argv[0], argv );

		perror("SSHPASS: Failed to run command");

		exit(RETURN_RUNTIME_ERROR);
	} else if( childpid < 0 ) {
		perror("SSHPASS: Failed to create child process");

		return RETURN_RUNTIME_ERROR;
	}

	// We are the parent
	close(pfd[1]);
	slavept=open(name, O_RDWR|O_NOCTTY );

	do {
		if( !terminate ) {
			fd_set readfd;
			int selret;
			FD_ZERO(&readfd);
			FD_SET(masterpt, &readfd);

			selret = pselect( masterpt + 1, &readfd, NULL, NULL, NULL, &sigmask_select );

			if( termsig != 0 ) {
				// Copying termsig isn't strictly necessary, as signals are masked at this point.
				int signum = termsig;
				termsig = 0;

				term_child(signum);

				continue;
			}

			if( selret > 0 ) {
				if( FD_ISSET( masterpt, &readfd ) ) {
					int ret;
					if( (ret = handleoutput( masterpt , password )) ) {
						// Authentication failed or any other error

						// handleoutput returns positive error number in case of some error, and a negative value
						// if all that happened is that the slave end of the pt is closed.
						if( ret > 0 ) {
							close( masterpt ); // Signal ssh that it's controlling TTY is now closed
							close( slavept );
						}

						terminate = ret;

						if( terminate ) {
							close( slavept );
						}
					}
				}
			}
			wait_id = waitpid( childpid, &status, WNOHANG );
		} else {
			wait_id = waitpid( childpid, &status, 0 );
		}
	} while( wait_id == 0 || (!WIFEXITED( status ) && !WIFSIGNALED( status )) );

	if( terminate > 0 )
		return terminate;
	/* WIFEXITED: 如果子进程通过调用 exit 或者一个返回 (return) 正常终止，就返回真 */
	else if( WIFEXITED( status ) )
		return 0;     /* modify by houjiaxing for #178952 at 2023/03/16 reveiwer huyuanni. */
	else
		return 255;
}

int handleoutput( int fd , const char *password )
{
	// We are looking for the string
	static int prevmatch = 0; // If the "password" prompt is repeated, we have the wrong password.
	static int state1, state2, state3;
	static const char *compare1 = "assword"; // Asking for a password
	static const char compare2[] = "The authenticity of host "; // Asks to authenticate host
	static const char compare3[] = "differs from the key for the IP address"; // Key changes
	// static const char compare3[]="WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!"; // Warns about man in the middle attack
	// The remote identification changed error is sent to stderr, not the tty, so we do not handle it.
	// This is not a problem, as ssh exists immediately in such a case
	char buffer[256];
	int ret = 0;

	int numread = read(fd, buffer, sizeof(buffer)-1 );
	buffer[numread] = '\0';

	state1 = match( compare1, buffer, numread, state1 );

	// Are we at a password prompt?
	if( compare1[state1] == '\0' ) {
		if( !prevmatch ) {
			reliable_write( fd, password, strlen( password ) );
			reliable_write( fd, "\n", 1 );
			state1 = 0;
			//prevmatch = 1;
		} else {
			// Wrong password - terminate with proper error code
			ret = RETURN_INCORRECT_PASSWORD;
		}
	}

	if( ret == 0 ) {
		state2 = match( compare2, buffer, numread, state2 );

		// Are we being prompted to authenticate the host?
		if( compare2[state2] == '\0' ) {
			ret = RETURN_HOST_KEY_UNKNOWN;
		} else {
			state3 = match( compare3, buffer, numread, state3 );
			// Host key changed
			if ( compare3[state3] == '\0' ) {
				ret = RETURN_HOST_KEY_CHANGED;
			}
		}
	}

	return ret;
}

int match( const char *reference, const char *buffer, ssize_t bufsize, int state )
{
	// This is a highly simplisic implementation. It's good enough for matching "Password: ", though.
	int i;
	for( i = 0; reference[state] != '\0' && i < bufsize; ++i ) {
		if( reference[state] == buffer[i] )
			state++;
		else {
			state = 0;
			if( reference[state] == buffer[i] )
				state++;
		}
	}

	return state;
}

void window_resize_handler(int signum)
{
	struct winsize ttysize; // The size of our tty

	if( ioctl( ourtty, TIOCGWINSZ, &ttysize ) == 0 )
		ioctl( masterpt, TIOCSWINSZ, &ttysize );
}

// Do nothing handler - makes sure the select will terminate if the signal arrives, though.
void sigchld_handler(int signum)
{
}

void term_handler(int signum)
{
	// BUG: There is a potential race here if two signals arrive before the main code had a chance to handle them.
	// This seems low enough risk not to justify the extra code to correctly handle this.
	termsig = signum;
}

void term_child(int signum)
{
	fflush(stdout);
	switch(signum) {
	case SIGINT:
		reliable_write(masterpt, "\x03", 1);
		break;
	case SIGTSTP:
		reliable_write(masterpt, "\x1a", 1);
		break;
	default:
		if( childpid > 0 ) {
			kill( childpid, signum );
		}
	}
}

void reliable_write( int fd, const void *data, size_t size )
{
	/*
	 * modify by houjiaxing for #178952 at 2023/03/16 reveiwer huyuanni.
	 * 仅保留模拟键入密码的方式
	 */
	ssize_t result = write( fd, data, size );
	if( result != size ) {
		if( result<0 ) {
			perror("SSHPASS: write failed");
		} else {
			fprintf(stderr, "SSHPASS: Short write. Tried to write %lu, only wrote %ld\n", size, result);
		}
	}
}
