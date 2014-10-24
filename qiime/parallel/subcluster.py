#!/usr/bin/env python
# File created on 14 Jul 2012
from __future__ import division

__author__ = "Nate Bresnick"
__copyright__ = "Copyright 2014, The QIIME project"
__credits__ = ["Nate Bresnick", "Jose Antonio Navas Molina"]
__license__ = "GPL"
__version__ = "1.8.0-dev"
__maintainer__ = "Nate Bresnick"
__email__ = "ndbresnick@gmail.com"

from os.path import join, abspath, exists
from os import makedirs

import networkx as nx
from biom import load_table
from cogent import LoadSeqs, DNA


from qiime.parallel.wrapper import ParallelWrapper
from qiime.workflow.util import (WorkflowLogger, call_commands_serially,
                                 no_status_updates)
from qiime.workflow.upstream import run_pick_de_novo_otus
from qiime.parse import fields_to_dict
from qiime.util import load_qiime_config


class ParallelSubcluster(ParallelWrapper):

    def _construct_job_graph(self, potu_table_fp, otu_map_fp, output_dir,
                             fasta_fp, params):
        """Constructs the workflow graph and the jobs to execute"""
        self._job_graph = nx.DiGraph()

        potu_table_fp = abspath(potu_table_fp)
        otu_map_fp = abspath(otu_map_fp)
        output_dir = abspath(output_dir)
        fasta_fp = abspath(fasta_fp)
        force = params["force"]

        qiime_config = load_qiime_config()

        # Create the log file
        self._logger = WorkflowLogger()

        # Create the output directory if it does not exists
        if not exists(output_dir):
            makedirs(output_dir)

        # Load the pOTU table
        potu_table = load_table(potu_table_fp)

        # get the seqs.fna for all the sequences in the whole set
        fasta_collection = LoadSeqs(fasta_fp, moltype=DNA, aligned=False,
                                    label_to_name=lambda x: x.split()[0])
        # get parent OTU map and load it into dict otu_to_seqid
        otu_to_seqid = fields_to_dict(open(otu_map_fp, 'U'))

        command_handler = call_commands_serially
        status_update_callback = no_status_updates

        for pOTU in potu_table.ids(axis='observation'):

            print "Reclustering pOTU# %s..." % str(pOTU)

            potu_dir = join(output_dir, str(pOTU))

            try:
                makedirs(potu_dir)
            except OSError:
                if force:
                    pass
                else:
                    # Since the analysis can take quite a while, I put this
                    # check in to help users avoid overwriting previous output.
                    raise OSError(
                        "Output directory already exists. Please choose "
                        "a different directory, or force overwrite with "
                        "--force")

            seqs_in_otu = fasta_collection.takeSeqs(otu_to_seqid[pOTU])
            output_fna_fp = join(potu_dir, 'seqs.fasta')
            seqs_in_otu.writeToFile(output_fna_fp)

            self._job_graph.add_node(pOTU, job=(run_pick_de_novo_otus,
                                                output_fna_fp, potu_dir,
                                                call_commands_serially, params,
                                                qiime_config, False, None,
                                                False, status_update_callback,
                                                False), requires_deps=False)
