// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Data-types used to represent the configuration of the various Strymon components.

pub mod job {
    //! Configuration data passed down to job processes.

    use std::env;
    use std::num;

    use JobId;

    /// The configuration of a job processes.
    ///
    /// This is typically passed down to the spawned process by the executor.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct Process {
        /// Job this process belongs to
        pub job_id: JobId,
        /// Index of this process (worker group)
        pub index: usize,
        /// Addresses of all worker groups of this same job
        pub addrs: Vec<String>,
        /// Number of thread this process hosts
        pub threads: usize,
        /// Address of the Strymon coordinator
        pub coord: String,
        /// Externally reachable hostname
        pub hostname: String,
    }

    const JOB_ID: &'static str = "STRYMON_JOB_CONF_ID";
    const PROCESS_INDEX: &'static str = "STRYMON_JOB_CONF_PROCESS_INDEX";
    const PROCESS_ADDRS: &'static str = "STRYMON_JOB_CONF_PROCESS_HOSTS";
    const PROCESS_THREADS: &'static str = "STRYMON_JOB_CONF_PROCESS_THREADS";
    const COORD: &'static str = "STRYMON_JOB_CONF_COORDINATOR";
    const HOSTNAME: &'static str = "STRYMON_JOB_CONF_HOSTNAME";

    impl Process {
        /// Decodes the process configuration from the environment, i.e. using `std::env::var`.
        pub fn from_env() -> Result<Self, EnvError> {
            Ok(Process {
                job_id: JobId::from(env::var(JOB_ID)?.parse::<u64>()?),
                index: env::var(PROCESS_INDEX)?.parse::<usize>()?,
                addrs: env::var(PROCESS_ADDRS)?
                    .split('|')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect(),
                threads: env::var(PROCESS_THREADS)?.parse::<usize>()?,
                coord: env::var(COORD)?,
                hostname: env::var(HOSTNAME)?,
            })
        }

        /// Flattens `self.addrs` of type `Vec<String>` into a pipe-separated string.
        fn join_addrs(&self) -> String {
            if self.addrs.is_empty() {
                String::new()
            } else {
                let cap = self.addrs.iter().map(|s| s.len()).sum::<usize>() + self.addrs.len();
                let mut joined = String::with_capacity(cap);

                let mut first = true;
                for s in &self.addrs {
                    if first {
                        first = false;
                    } else {
                        joined.push('|');
                    }
                    joined.push_str(s);
                }

                joined
            }
        }

        /// Encodes the process configuration into key-value pairs.
        ///
        /// Suitable for use with builders such as `Command::envs`.
        pub fn into_env(&self) -> Vec<(&'static str, String)> {
            vec![
                (JOB_ID, self.job_id.0.to_string()),
                (PROCESS_INDEX, self.index.to_string()),
                (PROCESS_ADDRS, self.join_addrs()),
                (PROCESS_THREADS, self.threads.to_string()),
                (COORD, self.coord.clone()),
                (HOSTNAME, self.hostname.clone()),
            ]
        }
    }

    /// Error which occurs when the job process configuration cannot be parsed
    /// from the local environment.
    #[derive(Debug)]
    pub enum EnvError {
        /// The configuration data could not be parsed because of a missing environment variable.
        VarErr(env::VarError),
        /// The configuration data contained invalid numbers.
        IntErr(num::ParseIntError),
    }

    impl From<env::VarError> for EnvError {
        fn from(var: env::VarError) -> Self {
            EnvError::VarErr(var)
        }
    }

    impl From<num::ParseIntError> for EnvError {
        fn from(int: num::ParseIntError) -> Self {
            EnvError::IntErr(int)
        }
    }

    #[cfg(test)]
    mod tests {
        use std::env;
        use JobId;
        use config::job::Process;

        #[test]
        fn env_encode_decode() {
            /// This helper stores and restores the configuration in the environment.
            fn assert_env_invariant(conf: Process) {
                for (k, v) in conf.into_env() {
                    env::remove_var(k);
                    env::set_var(k, v);
                }

                let restored = Process::from_env().expect("failed to restore");
                assert_eq!(conf, restored);

                for (k, _) in conf.into_env() {
                    env::remove_var(k);
                }
            }

            assert_env_invariant(Process {
                job_id: JobId(1),
                index: 0,
                addrs: vec![],
                threads: 1,
                coord: "foo".into(),
                hostname: "bar".into(),
            });
            assert_env_invariant(Process {
                job_id: JobId(2),
                index: 0,
                addrs: vec!["foo".into()],
                threads: 4,
                coord: "::1".into(),
                hostname: "bar".into(),
            });
            assert_env_invariant(Process {
                job_id: JobId(3),
                index: 1,
                addrs: vec!["host:1".into(), "host:2".into()],
                threads: 1,
                coord: "localhost".into(),
                hostname: "".into(),
            });
        }
    }
}
