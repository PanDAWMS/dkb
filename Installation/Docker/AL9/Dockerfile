# @project      The Data Knowledge Base  (DKB)
# @copyright    Copyright © 2024 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

# DKB generic image

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base

# DKB environment variables
ENV DKB_DIR="/opt/dkb3" \
    DKB_GIT="https://github.com/PanDAWMS/dkb.git" 

RUN yum install -y \
      git \
      wget \
      diffutils \
      libnsl \
      libaio \
      php \
      php-cli

# Prepare DKB software
RUN mkdir ${DKB_DIR} \
  && \
    cd ${DKB_DIR} \ 
  && \
    git clone ${DKB_GIT} dkb.git \
  && \
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
  && \
    sh ./Miniconda3-latest-Linux-x86_64.sh -b -p /opt/dkb3/miniconda3 -s -m \
  && \
    source "/opt/dkb3/miniconda3/etc/profile.d/conda.sh" \
  && \
    conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main \
  && \
    conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r \
  && \
    conda create -y -n dkb_python_3_9 python=3.9 \
  && \
    conda activate dkb_python_3_9 \
  && \
    conda install -y Elasticsearch cchardet cchardet cx_Oracle pytz\
  && \
    pip install pyAMI_core \
  && \
    pip install pyAMI_atlas \
  && \
    sed -e "s/ input(prompt)/'y'/" -i /opt/dkb3/miniconda3/envs/dkb_python_3_9/bin/ami_atlas_post_install \
  && \
    ami_atlas_post_install \
  && \
    pip install rucio \
  && \
    cd dkb.git/Utils/Dataflow/test/pyDKB \
  && \
    ./test.sh

# Patched version of pyAMI
COPY Installation/Docker/AL9/httpclient.py /opt/dkb3/miniconda3/envs/dkb_python_3_9/lib/python3.9/site-packages/pyAMI/httpclient.py

# Cleanup image
RUN yum clean all ; \
  rm -rf ${DKB_DIR}/Miniconda3-latest-Linux-x86_64.sh
