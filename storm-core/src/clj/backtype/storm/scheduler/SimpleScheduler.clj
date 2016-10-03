;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns backtype.storm.scheduler.SimpleScheduler
  (:use [backtype.storm util log config])
  (:require [clojure.set :as set])
  (:import [backtype.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot ExecutorDetails]
           [clojure.lang PersistentArrayMap])
  (:gen-class
    :implements [backtype.storm.scheduler.IScheduler]))

(defn sort-slots [all-slots]
  (let [split-up (sort-by count > (vals (group-by first all-slots)))]
    (apply interleave-all split-up)
    ))

(defn get-alive-assigned-node+port->executors [cluster topology-id]
  (let [existing-assignment (.getAssignmentById cluster topology-id)
        executor->slot (if existing-assignment
                         (.getExecutorToSlot existing-assignment)
                         {}) 
        executor->node+port (into {} (for [[^ExecutorDetails executor ^WorkerSlot slot] executor->slot
                                           :let [executor [(.getStartTask executor) (.getEndTask executor)]
                                                 node+port [(.getNodeId slot) (.getPort slot)]]]
                                       {executor node+port}))
        alive-assigned (reverse-map executor->node+port)]
    alive-assigned))

(defn- schedule-topology [^TopologyDetails topology ^Cluster cluster]
  (log-message "in Even-Scheduler: schedule-topology.")
  (let [topology-id (.getId topology)
        cluster-assignment (.getAssignmentById cluster topology-id)
        cluster-exec-to-slot (if cluster-assignment
                               (.getExecutorToSlot cluster-assignment)
                               {})
        cluster-execs (if cluster-assignment
                         (.getExecutors cluster-assignment)
                               {})
     ;;   cluster-executors (when-not (empty? cluster-exec-to-slot)
     ;;                          (.keySet() cluster-exec-to-slot))
        available-slots (->> (.getAvailableSlots cluster)
                             (map #(vector (.getNodeId %) (.getPort %))))
        all-executors (->> topology
                          .getExecutors
                          (map #(vector (.getStartTask %) (.getEndTask %)))
                          set)
        alive-assigned (get-alive-assigned-node+port->executors cluster topology-id)
        total-slots-to-use (min (.getNumWorkers topology)
                                (+ (count available-slots) (count alive-assigned)))
        reassign-slots (take (- total-slots-to-use (count alive-assigned))
                             (sort-slots available-slots))
        reassign-executors (sort (set/difference all-executors (set (apply concat (vals alive-assigned)))))
        reassignment (into {}
                           (map vector
                                reassign-executors
                                ;; for some reason it goes into infinite loop without limiting the repeat-seq
                                (repeat-seq (count reassign-executors) reassign-slots)))]

    (when-not (empty? reassignment)
      (log-message "reassignment is not empty :)")
      (log-message "Topology-id " (pr-str topology-id) " At the end of schedule-topology " (pr-str available-slots) " all-executors " (pr-str all-executors) " alive-assigned " (pr-str alive-assigned) " total-slots-to-use " (pr-str total-slots-to-use) " reassignment " (pr-str reassignment) )
      (log-message "Available slots: " (pr-str available-slots))
      )
    (when (empty? reassignment)
      (log-message "Topology-id " (pr-str topology-id))
      (log-message "Type of reassignment " (pr-str (type reassignment)))
      (log-message "Type of cluster-exec-to-slot " (pr-str (type cluster-exec-to-slot)))
      (log-message "Type of casted cluster-exec-to-slot "(pr-str (type (PersistentArrayMap/create cluster-exec-to-slot))))
      (log-message "Printing (set (apply concat (vals alive-assigned))) in topology " (pr-str (set (apply concat (vals alive-assigned)))))
      (log-message "Printing all-executors in topology " (pr-str all-executors))
      (log-message "Printing reassign-executors " (pr-str reassign-executors))
      (log-message "Printing cluster-top-executors " (pr-str cluster-execs))
      (log-message "Printing cluster-exec-to-slot " (pr-str cluster-exec-to-slot) )
    ;;  (log-message "Printing cluster-executors " (pr-str cluster-executors))
      (log-message "Reassignment is empty inside the scheduler"))
   ;; reassignment))
    (PersistentArrayMap/create cluster-exec-to-slot)))

(defn schedule-topologies-evenly [^Topologies topologies ^Cluster cluster]
  (log-message "in schedule-topologies-evenly")
  (let [needs-scheduling-topologies (.needsSchedulingTopologies cluster topologies)]
    (doseq [^TopologyDetails topology needs-scheduling-topologies
            :let [topology-id (.getId topology)
                  cluster-assigment (.getAssignmentById cluster topology-id)
                  cluster-exec-to-slot (if cluster-assigment
                                         (.getExecutors cluster-assigment)
                                         {})]
            ]
      (log-message "Printing for topology first " topology-id " cluster-exec-to-slot from schedule-topologies-evenly " (pr-str cluster-exec-to-slot))
      )
    )
  (let [needs-scheduling-topologies (.needsSchedulingTopologies cluster topologies)]
    (doseq [^TopologyDetails topology needs-scheduling-topologies
            :let [topology-id (.getId topology)
                    cluster-assigment (.getAssignmentById cluster topology-id)
                    cluster-exec-to-slot (if cluster-assigment
                             (.getExecutors cluster-assigment)
                             {})
                  new-assignment (schedule-topology topology cluster)
                  node+port->executors (reverse-map new-assignment)]
             ]
      (doseq [[node+port executors] node+port->executors]
        (log-message "node + port " (pr-str node+port) " executors " (pr-str executors)))
      (doseq [[node+port executors] node+port->executors
              ;;:let [^WorkerSlot slot (WorkerSlot. (first node+port) (last node+port))
                 ;;   executors (for [[start-task end-task] executors]
                   ;;             (ExecutorDetails. start-task end-task))]
              ]
        (log-message "node + port " (pr-str node+port) " executors " (pr-str executors))
        (.assign cluster node+port topology-id executors))
      (log-message "Printing for topology " topology-id " cluster-exec-to-slot from schedule-topologies-evenly " (pr-str cluster-exec-to-slot))
      )
     )
  )

(defn -prepare [this conf]
  )

(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (log-message "schedule for even scheduler")
  (let [needs-scheduling-topologies (.needsSchedulingTopologies cluster topologies)]
    (doseq [^TopologyDetails topology needs-scheduling-topologies
            :let [topology-id (.getId topology)
                  cluster-assigment (.getAssignmentById cluster topology-id)
                  cluster-exec-to-slot (if cluster-assigment
                                         (.getExecutors cluster-assigment)
                                         {})]
            ]
      (log-message "Printing for topology from schedule function " topology-id " cluster-exec-to-slot from schedule-topologies-evenly " (pr-str cluster-exec-to-slot))
      )
    )
  (schedule-topologies-evenly topologies cluster))
