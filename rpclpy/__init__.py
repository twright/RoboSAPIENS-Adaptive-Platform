"""
RPCLPY - RoboSapiens Client Library for Python

A library for creating nodes that can communicate with each other using
messages, events, and knowledge management.
"""

from .node import Node
from .CommunicationManager import CommunicationManager
from .KnowledgeManager import KnowledgeManager
from .LoggingAndTracking import LoggingAndTrackingHandler

__version__ = "1.0.0"
__author__ = "RoboSapiens"

__all__ = [
    "Node",
    "CommunicationManager", 
    "KnowledgeManager",
    "LoggingAndTrackingHandler"
]