@echo off
set PATH=cwrsync_6.3.0_x64_free\bin;%PATH%
rem rsync "D:\Projets\Cumulus\cumulus_rsync\.cumulus.rsync" "134.158.151.45:/storage/jobs/Job_133_Burel.Alexandre_diann_2.0_1740491647"
rem rsync -r --ignore-existing --exclude='*-wal' --progress -e 'ssh -l ubuntu -i "D:\Projets\Cumulus\cumulus_rsync\cumulus.pem" -o "StrictHostKeyChecking no"' --chmod=Du=rwx,Dg=rx,Do=rx,Fu=rw,Fg=r,Fo=r ".cumulus.rsync" "134.158.151.45:/storage/jobs/Job_133_Burel.Alexandre_diann_2.0_1740491647"
rsync -r --ignore-existing --exclude='*-wal' --progress -e 'ssh -l ubuntu -i "D:\Projets\Cumulus\cumulus_rsync\cumulus.pem" -o "StrictHostKeyChecking no"' --chmod=Du=rwx,Dg=rx,Do=rx,Fu=rw,Fg=r,Fo=r "//maxquant/d$/DATA/Perdu-Alloy Pauline/Human_pSP_CMO_20190213.fasta" "134.158.151.45:/storage/jobs/Job_134_Burel.Alexandre_diann_2.0_1740493430"
rem The source and destination cannot both be remote.
rem rsync error: syntax or usage error (code 1) at main.c(1415) [Receiver=3.3.0]
pause