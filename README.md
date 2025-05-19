# Cumulus RSync Agent
 
A Flask REST API for Cumulus.

This module of Cumulus will transfer all the data to the server, using RSync to reduce the amount of transfer when it's possible.
The reason for a separate module, rather than transferring the data from the client, is to avoid parallel transfers, as well as to make sure the files are transferred from the best place in your local network. For instance, if your data are stored on a shared folder in the network, it is best to run CuRSA directly on the server hosting that shared folder and transfer the data from there.
Another reason is that RSync requires a certificate to connect to the server, using a separate module avoids sharing that certificate with the users.


## Installation

To run the Cumulus RSync Agent work, you need to:

* Provide a working certificate to contact the controller where Cumulus_server is listening. At the moment, the expected certificate cannot require a password.
    * Make sure the permissions for the certificate are not too open.
    * Linux users should use "chmod 600"
    * On Windows the inheritance should be disabled and all ACL deleted, especially for "Users" and "Authenticated Users", then add a specific user account and only allow Read permission
* Make sure to have RSync installed and added to the PATH. On Windows machines, use https://itefix.net/cwrsync
* Fill the cumulus_rsync.conf file
* It is advised to create a service to make sure the agent will be working at all times. On Windows, you can use NSSM: https://nssm.cc/


## What is Cumulus

Cumulus is a three-part tool whose purpose is to run software on a Cloud, without requiring the users to have any computational knowledge. Cumulus is made of three elements: 
* [A client](https://github.com/LSMBO/cumulus-client) with a graphical user interface developped in Javascript using ElectronJS.
* [A server application](https://github.com/LSMBO/cumulus_server), running on the Cloud. The server will dispatch the jobs, monitor them, and store everything in a database. The server is developped in Python and provides a Flast REST API for the client and the agent.
* [A separate agent](https://github.com/LSMBO/cumulus_rsync) who manages the transfer of the files from client to server. The agent is developped in Python and provides a Flask REST API for the client.

Cumulus has been developped to work around the [SCIGNE Cloud at IPHC](https://scigne.fr/), it may require some modifications to work on other Clouds, depending on how the virtual machines are organized. The virtual machines currently in use for Cumulus are set up like this:
* A controller with 4 VCPU, 8GB RAM, 40GB drive, this is where cumulus-server is running. This VM is the only one with a public IP address.
* Four virtual machines with 16 to 64VCPU and 64 to 256GB RAM, this is where the jobs will be running. These VM can be accessed by the controller using SSH.
* A 15TB storage unit, mounted as a NFS shared drive on every VM so the content is shared with the same path. This is where the data, the jobs and apps will be stored.

The virtual machines on the Cloud are all running with Ubuntu 24.04, Cumulus has only been tested there so it's possible that some scripts may not work on a different Linux distribution.

The Agent's purpose is to provide a single queue to ease the transfer of large files. Cumulus has been developped to run applications dealing with mass spectrometry data, which are often between 1 and 10GB. Cumulus has been developped for a use in a work environment with a dedicated network, where data are stored on a server, and users are running apps on their own sessions. In that context, it is more effective to transfer data from the server directly, rather than from each user's session. The queue is stored in a local database, so it does not disappear if the agent has to be restarted.

The agent has been tested on a Windows server, and uses [cwRsync](https://www.itefix.net/cwrsync), but it should work on a Linux server using a local RSync command. Scripts to create a Windows service are provided.

