### TODO

* [x] DirectoryService Drop expired services
* [x] ZReqRepService send alive to directory service
* [x] Make RoutedService.routes, ZReqRepService.handler as Interface
* [ ] Use one name either 'alive' or 'heartbeat'
* [ ] Organize derectory structure
* [ ] Make python package
* [ ] Service should re-register if heartbeat returns UnknownNode
* [ ] Discard expired services after a while
* [ ] DirectoryService Pub service state, diff and snapshot
* Add services:
    * [x] Web interface
    * [ ] Remote directory discovery. What is global path
    * [ ] One-point registry for remote discovery
    * [ ] Load balancing
    * [ ] Measure network latencies and bandwith to
        remote directories and workers. Add this info to
        directory.
    * [ ] Start and scale services on demand
* [ ] Service should listen for changes from directory,
    know remote directories and move to it when current
    is gone, or move to closer directory if it is available.
* [ ] Interconnections of same service workers, share state
* [ ] Warm shutdown and restart service
* [ ] Develop dependencies in Interface (What services does Interface depend on)
* [ ] Write protocol specification
* [ ] License
