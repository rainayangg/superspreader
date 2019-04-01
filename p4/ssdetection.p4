/* -*- P4_16 -*- */
/*
  Implementation of the superspreader detection algorithm. Please skip to the egress pipeline.
  Released under Apache 2.0 license. You may obtain a copy of Apache 2.0 at https://www.apache.org/licenses/LICENSE-2.0
*/

#define FLOW_TABLE_SIZE_EACH 1024
#define HASH_BASE 10w0
#define HASH_MAX 10w1023




#include <core.p4>
#include <v1model.p4>

const bit<16> TYPE_IPV4 = 0x800;

// Headers and Metadata definitions

typedef bit<9>  egressSpec_t;
typedef bit<48> macAddr_t;
typedef bit<32> ip4Addr_t;

header ethernet_t {
    macAddr_t dstAddr;
    macAddr_t srcAddr;
    bit<16>   etherType;
}

header ipv4_t {
    bit<4>    version;
    bit<4>    ihl;
    bit<8>    diffserv;
    bit<16>   totalLen;
    bit<16>   identification;
    bit<3>    flags;
    bit<13>   fragOffset;
    bit<8>    ttl;
    bit<8>    protocol;
    bit<16>   hdrChecksum;
    ip4Addr_t srcAddr;
    ip4Addr_t dstAddr;
}

struct custom_metadata_t {
	// source IP address
	bit<32> my_sourceID;
	// dest IP address
	bit<32> my_destID;
	// bloom filter
	bit<64> bloomfilter;
	// whether it is matched in table
	bit<1> already_matched;

	// the min count
	bit<64> carry_min;
	// the min stage to replace
	bit<8> min_stage;
	// whether we recirculate the packet
	bit<1> do_recirculate;
	// whether the recirculate is due to min replacement 0 
	// or due to duplicate removal 1
	bit<1> recirculate_type;
	bit<9> orig_egr_port;

	// hashed address on table
	bit<32> hashed_address_s1;
	bit<32> hashed_address_s2;
	bit<32> hashed_address_s3;

	// hashed destination address to bloomfilter index
	bit<64> hashed_bloomfilter_addr;

	bit<64> random_bits;
	bit<12> random_bits_short;

	// current counter of superspreader
	bit<64> current_count;
}

struct headers {
    ethernet_t   ethernet;
    ipv4_t       ipv4;
}

// Standard IPv4 parser

parser MyParser(packet_in packet,
                out headers hdr,
                inout custom_metadata_t meta,
                inout standard_metadata_t standard_metadata) {

    state start {
        transition parse_ethernet;
    }

    state parse_ethernet {
        packet.extract(hdr.ethernet);
        transition select(hdr.ethernet.etherType) {
            TYPE_IPV4: parse_ipv4;
            default: accept;
        }
    }

    state parse_ipv4 {
        packet.extract(hdr.ipv4);
        transition accept;
    }
}

control MyVerifyChecksum(inout headers hdr, inout custom_metadata_t meta) {
    apply {  }
}


control MyIngress(inout headers hdr,
                  inout custom_metadata_t meta,
                  inout standard_metadata_t standard_metadata) {
	// We did some rudimentary forwarding in the ingress pipeline, only for demo purpose.
    action repeater () {
		// Under basic mininet setup in P4 tutorial, we have one switch and two hosts, at port 1/2.
		// We repeat packets from h1 to h2 and vice virsa.
        standard_metadata.egress_spec = 9w3 - standard_metadata.ingress_port;
    }
	action bouncer () {
		// We reflect all packets to its sender.
        standard_metadata.egress_spec = standard_metadata.ingress_port;
    }

    apply {
		if(hdr.ipv4.isValid()){
			// Choose from repeater or bouncer, or add your real IPv4 forwarding?
			repeater();
		}else{
			mark_to_drop();
		}
	}
}

// =========== Start implementation of Superspreader Detection ============
// A brief introduction to algorithm:
// 1. We maintain many (source IP, bloomfilter, distinct destnation count) tuples
// 2. When new packet comes and the source IP is in the table, we update the bloomfilter and 
// counter based on the destination IP. Otherwise we replace the entry with minimum count.
// 3. We use recirculation to evict entry and evict with probability 1/(min+1).
// In this example, we implement d=3.

control MyEgress(inout headers hdr,
                 inout custom_metadata_t meta,
				 inout standard_metadata_t standard_metadata) {
	register<bit<32> >(FLOW_TABLE_SIZE_EACH)  flow_table_ids_1;
	register<bit<64> >(FLOW_TABLE_SIZE_EACH) flow_table_bloomfilter_1;
	register<bit<64> >(FLOW_TABLE_SIZE_EACH)  flow_table_ctrs_1;
	register<bit<32> >(FLOW_TABLE_SIZE_EACH)  flow_table_ids_2;
	register<bit<64> >(FLOW_TABLE_SIZE_EACH) flow_table_bloomfilter_2;
	register<bit<64> >(FLOW_TABLE_SIZE_EACH)  flow_table_ctrs_2;
	register<bit<32> >(FLOW_TABLE_SIZE_EACH)  flow_table_ids_3;
	register<bit<64> >(FLOW_TABLE_SIZE_EACH) flow_table_bloomfilter_3;
	register<bit<64> >(FLOW_TABLE_SIZE_EACH)  flow_table_ctrs_3;

	// compute source IP
	action commpute_source_id () {
		meta.my_sourceID=hdr.ipv4.srcAddr;
	}

	// compute dest IP
	action commpute_dest_id () {
		meta.my_destID=hdr.ipv4.dstAddr;
	}

	action compute_reg_index () {
		// table index
		hash(meta.hashed_address_s1, HashAlgorithm.crc16, HASH_BASE,
			 	{hdr.ipv4.srcAddr, 7w11}, HASH_MAX);

		hash(meta.hashed_address_s2, HashAlgorithm.crc16, HASH_BASE,
			 	{hdr.ipv4.srcAddr, 5w3}, HASH_MAX);

		hash(meta.hashed_address_s3, HashAlgorithm.crc16, HASH_BASE,
			 	{hdr.ipv4.srcAddr, 1w1}, HASH_MAX);
	}

	// compute the hash index in bloomfilter based on dstAddr
	action compute_bloom_index () {
		// assume hash to a bit string where it has 1 on the hashed index
		hash(meta.hashed_bloomfilter_addr, HashAlgorithm.crc16, HASH_BASE,
			 	{hdr.ipv4.dstAddr}, HASH_MAX);
	}

	action clone_and_recirc_replace_entry(){
		// We need to set up a mirror ID in order to make this work.
		// Use simple_switch_CLI: mirroring_add 0 0
		#define MIRROR_ID 0
		clone3<custom_metadata_t>(CloneType.E2E, MIRROR_ID, meta);
		// Recirculated packets carried meta.min_stage and meta.count_min, so they themselves know what to do.
	}

	apply {
		commpute_source_id();
		commpute_dest_id();
		compute_bloom_index();
		compute_reg_index();
		// source IP
		bit<32> tmp_existing_source_id;
		// bloomfilter
		bit<64> tmp_existing_bloomfilter;
		// distinct destination count
		bit<64> tmp_existing_dest_count;

		if(standard_metadata.instance_type==0){
			// Regular incoming packets.
			// We check if the flow ID is already in the flow table.
			// If not, we remember the minimum counter we've seen so far.

			// access table 1
			flow_table_ids_1.read(tmp_existing_source_id, meta.hashed_address_s1);
			flow_table_bloomfilter_1.read(tmp_existing_bloomfilter, meta.hashed_address_s1);
			flow_table_ctrs_1.read(tmp_existing_dest_count, meta.hashed_address_s1);

			// check if source IP maps to empty entry or entry with same source IP
			if(tmp_existing_dest_count==0 || tmp_existing_source_id==meta.my_sourceID){
				flow_table_ids_1.write(meta.hashed_address_s1, meta.my_sourceID);
				meta.current_count=tmp_existing_dest_count;
				// update bloomfilter and counter only if the source does not map to a bit of 1
				if (tmp_existing_bloomfilter & meta.hashed_bloomfilter_addr==0){
					// 00010 | 01000 = 01010 
					tmp_existing_bloomfilter=tmp_existing_bloomfilter | meta.hashed_bloomfilter_addr;
					flow_table_bloomfilter_1.write(meta.hashed_address_s1, tmp_existing_bloomfilter);
					flow_table_ctrs_1.write(meta.hashed_address_s1, tmp_existing_dest_count+1);
					meta.current_count=tmp_existing_dest_count+1;
				}
				meta.already_matched=1;

			}else{
				// if count is 1, directly replace without recirculate
				if(tmp_existing_dest_count==1){
					flow_table_ids_1.write(meta.hashed_address_s1, meta.my_sourceID);
					tmp_existing_bloomfilter=tmp_existing_bloomfilter | meta.hashed_bloomfilter_addr;
					flow_table_bloomfilter_1.write(meta.hashed_address_s1, tmp_existing_bloomfilter);
					flow_table_ctrs_1.write(meta.hashed_address_s1, 1);
					meta.current_count=1;
				}else{
					//save min_stage
					//special case for first stage: always min
					meta.carry_min=tmp_existing_dest_count;
					meta.min_stage=1;
				}
			}

			// access table 2
			if(meta.already_matched==0){
				// access table 2
				flow_table_ids_2.read(tmp_existing_source_id, meta.hashed_address_s2);
				flow_table_bloomfilter_2.read(tmp_existing_bloomfilter, meta.hashed_address_s2);
				flow_table_ctrs_2.read(tmp_existing_dest_count, meta.hashed_address_s2);

				// check if source IP maps to empty entry or entry with same source IP
				if(tmp_existing_dest_count==0 || tmp_existing_source_id==meta.my_sourceID){
					flow_table_ids_2.write(meta.hashed_address_s2, meta.my_sourceID);
					meta.current_count=tmp_existing_dest_count;
					// update bloomfilter and counter only if the source does not map to a bit of 1
					if (tmp_existing_bloomfilter & meta.hashed_bloomfilter_addr==0){
						tmp_existing_bloomfilter = tmp_existing_bloomfilter | meta.hashed_bloomfilter_addr;
						flow_table_bloomfilter_2.write(meta.hashed_address_s2, tmp_existing_bloomfilter);
						flow_table_ctrs_2.write(meta.hashed_address_s2, tmp_existing_dest_count+1);
						meta.current_count=tmp_existing_dest_count+1;
					}
					meta.already_matched=1;

				}else{
					// if count is 1, directly replace without recirculate
					if(tmp_existing_dest_count==1){
						flow_table_ids_2.write(meta.hashed_address_s2, meta.my_sourceID);
						tmp_existing_bloomfilter=tmp_existing_bloomfilter|meta.hashed_bloomfilter_addr;
						flow_table_bloomfilter_2.write(meta.hashed_address_s2, tmp_existing_bloomfilter);
						flow_table_ctrs_2.write(meta.hashed_address_s2, 1);
						meta.current_count=1;
						meta.already_matched=1;
					}else{
						// save min_stage
						if(meta.carry_min>tmp_existing_dest_count){
							meta.carry_min=tmp_existing_dest_count;
							meta.min_stage=2;
						}
					}
				}
			}
			else{
				// duplicate removal, remove duplicate in the future pipline in found
				if(tmp_existing_source_id==meta.my_sourceID){
					if(tmp_existing_dest_count<meta.current_count){
						meta.min_stage=2;
					}
					meta.do_recirculate=1;
					meta.recirculate_type=1;
					clone_and_recirc_replace_entry();
				}

			}

			// access table 3
			if(meta.already_matched==0){
				// access table 3
				flow_table_ids_3.read(tmp_existing_source_id, meta.hashed_address_s3);
				flow_table_bloomfilter_3.read(tmp_existing_bloomfilter, meta.hashed_address_s3);
				flow_table_ctrs_3.read(tmp_existing_dest_count, meta.hashed_address_s3);

				// check if source IP maps to empty entry or entry with same source IP
				if(tmp_existing_dest_count==0 || tmp_existing_source_id==meta.my_sourceID){
					flow_table_ids_3.write(meta.hashed_address_s3, meta.my_sourceID);
					meta.current_count=tmp_existing_dest_count;
					// update bloomfilter and counter only if the source does not map to a bit of 1
					if (tmp_existing_bloomfilter & meta.hashed_bloomfilter_addr==0){
						tmp_existing_bloomfilter=tmp_existing_bloomfilter | meta.hashed_bloomfilter_addr;
						flow_table_bloomfilter_3.write(meta.hashed_address_s3, tmp_existing_bloomfilter);
						flow_table_ctrs_3.write(meta.hashed_address_s3, tmp_existing_dest_count+1);
						meta.current_count=tmp_existing_dest_count+1;
					}
					meta.already_matched=1;

				}else{
					// if count is 1, directly replace without recirculate
					if(tmp_existing_dest_count==1){
						flow_table_ids_3.write(meta.hashed_address_s3, meta.my_sourceID);
						tmp_existing_bloomfilter = tmp_existing_bloomfilter | meta.hashed_bloomfilter_addr;
						flow_table_bloomfilter_3.write(meta.hashed_address_s3, tmp_existing_bloomfilter);
						flow_table_ctrs_3.write(meta.hashed_address_s3, 1);
						meta.current_count=1;
						meta.already_matched=1;
					}else{
						// save min_stage
						if(meta.carry_min>tmp_existing_dest_count){
							meta.carry_min=tmp_existing_dest_count;
							meta.min_stage=3;
						}
					}
				}
			}
			else{
				// duplicate removal, remove duplicate in the future pipline in found
				if(tmp_existing_source_id==meta.my_sourceID){
					if(tmp_existing_dest_count<meta.current_count){
						meta.min_stage=3;
					}
					meta.do_recirculate=1;
					meta.recirculate_type=1;
					clone_and_recirc_replace_entry();
				}

			}

			// decide to recirculate or not...
			if(meta.already_matched==0){
				// Ideally, we need to recirculate with probability 1/(carry_min*100).
				// There are three options for probabilistic recircuation.
				// 1. Perfect probability: use precisely 1/(carry_min*100) probabilty. May not be supported by all hardware.
				//#define PERFECT_PROBABILITY
				#define BETTER_APPROXIMATE
				#if defined(PERFECT_PROBABILITY)
				{
					bit<64> rnd;
					random<bit<64> >(rnd,64w0,meta.carry_min*100);
					if(rnd==0){
						clone_and_recirc_replace_entry();
					}

				}
				#endif
			}



		}else{
			// This packet is a recirculated packet.
			// Since we use Clone and Recirculate / Clone and Resubmit, we always drop them.
			mark_to_drop();
			if(meta.recirculate_type==0){ // recirculate to replace min
				// We replace the source IP in tables, and set counter to 1 and corresponding bloomfilter.
				if(meta.min_stage==1){
					flow_table_ids_1.write(meta.hashed_address_s1, meta.my_sourceID);
					meta.bloomfilter = meta.bloomfilter | meta.hashed_bloomfilter_addr;
					flow_table_bloomfilter_1.write(meta.hashed_address_s1, meta.bloomfilter);
					flow_table_ctrs_1.write(meta.hashed_address_s1, 1);
				}
				if(meta.min_stage==2){
					flow_table_ids_2.write(meta.hashed_address_s2, meta.my_sourceID);
					meta.bloomfilter = meta.bloomfilter | meta.hashed_bloomfilter_addr;
					flow_table_bloomfilter_2.write(meta.hashed_address_s2, meta.bloomfilter);
					flow_table_ctrs_2.write(meta.hashed_address_s2, 1);			
				}
				if(meta.min_stage==3){
					flow_table_ids_3.write(meta.hashed_address_s3, meta.my_sourceID);
					meta.bloomfilter = meta.bloomfilter | meta.hashed_bloomfilter_addr;
					flow_table_bloomfilter_3.write(meta.hashed_address_s3, meta.bloomfilter);
					flow_table_ctrs_3.write(meta.hashed_address_s3, 1);		
				}
			}else{ //recirculate to remove duplicate
				if(meta.min_stage==1){
					flow_table_ids_1.write(meta.hashed_address_s1, 0);
					bit<64> new_bloomfilter=0;
					flow_table_bloomfilter_1.write(meta.hashed_address_s1, new_bloomfilter);
					flow_table_ctrs_1.write(meta.hashed_address_s1, 0);
				}
				if(meta.min_stage==2){
					flow_table_ids_2.write(meta.hashed_address_s2, 0);
					bit<64> new_bloomfilter=0;
					flow_table_bloomfilter_2.write(meta.hashed_address_s2, new_bloomfilter);
					flow_table_ctrs_2.write(meta.hashed_address_s2, 0);	
				}
				if(meta.min_stage==3){
					flow_table_ids_3.write(meta.hashed_address_s3, 0);
					bit<64> new_bloomfilter=0;
					flow_table_bloomfilter_3.write(meta.hashed_address_s3, new_bloomfilter);
					flow_table_ctrs_3.write(meta.hashed_address_s1, 0);	
				}

			}

		}


		// Write result to packet header for demo purpose.
		// In actual applications, we can use the estimated count for decision making in data plane.
		hdr.ethernet.dstAddr = 0xffffffffffff;
		hdr.ethernet.srcAddr = 0;
	}
}

// =========== End implementation of PRECISION ===========

// We did not change header, no need to recompute checksum
control MyComputeChecksum(inout headers hdr, inout custom_metadata_t meta) {
     apply { }
}


// Minimal deparser etc.
control MyDeparser(packet_out packet, in headers hdr) {
    apply {
        packet.emit(hdr.ethernet);
        packet.emit(hdr.ipv4);
    }
}

V1Switch(
MyParser(),
MyVerifyChecksum(),
MyIngress(),
MyEgress(),
MyComputeChecksum(),
MyDeparser()
) main;
