#!/usr/bin/env python
# coding: utf-8

import os
import sys
import traceback
from contextlib import closing
from typing import Optional

import click
import configparser
import h2o_authn
import h2osteam

from h2oaicore.systemutils import main_logger as logger
from h2osteam.clients import DriverlessClient


class ConfigException(Exception):
    """Exception raised for errors in the configuration."""


class InstanceNotFoundException(Exception):
    """Exception raised when an instance is not found."""


class Instance:
    """Represents a Driverless AI instance."""

    def __init__(self, instance):
        self.instance = instance

    @property
    def id(self):
        return self.instance.id

    @property
    def status(self):
        return self.instance.status

    def stop(self):
        self.instance.stop()

    def start(self, resources):
        self.instance.start(**resources)

    def connect(self):
        return self.instance.connect()


class Config:
    """Represents a configuration for HAIC Steam."""

    def __init__(self, steam_url: str):
        self.steam_url = steam_url.rstrip("/")
        self._config = self._load_config()

    def _load_config(self):
        config_file = 'h2oai.config'
        if not os.path.exists(config_file):
            raise ConfigException(f"Config file does not exist: {config_file}")
        config = configparser.ConfigParser()
        config.read(config_file)
        return config[self.steam_url]

    @property
    def refresh_token(self):
        return self._config.get("refresh_token")

    @property
    def client_id(self):
        return self._config.get("client_id")

    @property
    def token_endpoint(self):
        return self._config.get("token_endpoint")


class InstanceService:
    """Provides services for handling instances."""

    def __init__(self, config: Config):
        self.config = config

    def steam_login(self):
        with closing(h2o_authn.TokenProvider(
            refresh_token=self.config.refresh_token,
            client_id=self.config.client_id,
            token_endpoint_url=self.config.token_endpoint
        )) as token_provider:
            h2osteam.login(url=self.config.steam_url, access_token=token_provider())

    def get_instance(self, instance_name: str, owner: str):
        instance = DriverlessClient().get_instance(name=instance_name, created_by=owner)
        if instance is None:
            raise InstanceNotFoundException(f"Instance '{instance_name}' belonging to '{owner}' does not exist.")
        return Instance(instance)

    def set_instance_owner(self, instance: Instance, owner: str):
        h2osteam.api().set_driverless_instance_owner(instance.id, owner)

    def transfer_entities(self, instance: Instance, username_from: str, username_to: str):
        dai_admin = instance.connect()._backend.admin
        dai_admin.transfer_entities(username_from=username_from, username_to=username_to)


class CommandHandler:
    """Handles the command line inputs and uses the services provided by InstanceService."""

    def __init__(self, instance_service: InstanceService):
        self.service = instance_service

    def handle(self, admin_user_name, instance_name, old_user_name, new_user_name, instance_cpu, instance_mem):
        self.service.steam_login()

        click.confirm(
            f"Do you want to transfer the instance '{instance_name}' from '{old_user_name}' to '{new_user_name}' (this will shut the instance down and interrupt any running jobs)?",
            abort=True
        )

        instance = self.service.get_instance(instance_name, old_user_name)
        self.service.set_instance_owner(instance, admin_user_name)
        if instance.status == "running":
            instance.stop()
        resources = {"cpu_count": instance_cpu, "memory_gb": instance_mem}
        instance.start(resources)
        self.service.transfer_entities(instance, old_user_name, new_user_name)
        instance.stop()
        self.service.set_instance_owner(instance, new_user_name)
        logger.info("Transfer complete.")

def parse_command_line_options():
    """
    Parse command line options and return them as a dictionary.
    """
    options = {
        'admin_user_name': click.option('--admin-user-name', help='HAIC user name of Steam administrator invoking the transfer.', required=True),
        'steam_url': click.option('--steam-url', help='HAIC Steam URL.', required=True),
        'instance_name': click.option('--instance-name', help='Name of Driverless AI instance on Steam to transfer.', required=True),
        'old_user_name': click.option('--old-user-name', help='HAIC user name of instance owner.', required=True),
        'new_user_name': click.option('--new-user-name', help='HAIC user name to transfer instance ownership to.', required=True),
        'instance_cpu': click.option('--instance-cpu', help='Use if transfer stalls because instance cpu is too low.', default=None, required=False),
        'instance_mem': click.option('--instance-mem', help='Use if transfer stalls because instance mem is too low.', default=None, required=False),
    }
    return options


@click.command()
@click.pass_context
def instance_transfer(ctx):
    """
    Transfer instance ownership from one user to another.
    This script requires an 'h2oai.config' file in the current working directory.
    """
    try:
        options = parse_command_line_options()
        config = Config(options['steam_url'])
        service = InstanceService(config)
        handler = CommandHandler(service)
        handler.handle(
            options['admin_user_name'], 
            options['instance_name'], 
            options['old_user_name'], 
            options['new_user_name'], 
            options['instance_cpu'], 
            options['instance_mem']
        )
    except Exception:
        t, v, tb = sys.exc_info()
        ex = "".join(traceback.format_exception(t, v, tb))
        logger.error(ex)
        sys.exit(1)


if __name__ == "__main__":
    instance_transfer()
