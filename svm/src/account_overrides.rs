use {
    dashmap::{mapref::one::Ref, DashMap},
    solana_account::AccountSharedData,
    solana_pubkey::Pubkey,
    solana_sdk_ids::sysvar,
};

/// Encapsulates overridden accounts, typically used for transaction
/// simulations. Account overrides are currently not used when loading the
/// durable nonce account or when constructing the instructions sysvar account.
#[derive(Clone, Default, Debug)]
pub struct AccountOverrides {
    accounts: DashMap<Pubkey, AccountSharedData>,
}

impl AccountOverrides {
    pub fn upsert_account_overrides(&mut self, other: AccountOverrides) {
        self.accounts.extend(other.accounts);
    }

    /// Insert or remove an account with a given pubkey to/from the list of overrides.
    pub fn set_account(&self, pubkey: &Pubkey, account: Option<AccountSharedData>) {
        match account {
            Some(account) => self.accounts.insert(*pubkey, account),
            None => self.accounts.remove(pubkey).map(|(_, account)| account),
        };
    }

    /// Sets in the slot history
    ///
    /// Note: no checks are performed on the correctness of the contained data
    pub fn set_slot_history(&self, slot_history: Option<AccountSharedData>) {
        self.set_account(&sysvar::slot_history::id(), slot_history);
    }

    /// Gets the account if it's found in the list of overrides
    pub fn get(&self, pubkey: &Pubkey) -> Option<Ref<Pubkey, AccountSharedData>> {
        self.accounts.get(pubkey)
    }

    pub fn len(&self) -> usize {
        self.accounts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }

    pub fn accounts(&self) -> &DashMap<Pubkey, AccountSharedData> {
        &self.accounts
    }
}

#[cfg(test)]
mod test {
    use {
        crate::account_overrides::AccountOverrides, solana_account::AccountSharedData,
        solana_pubkey::Pubkey, solana_sdk_ids::sysvar,
    };

    #[test]
    fn test_set_account() {
        let accounts = AccountOverrides::default();
        let data = AccountSharedData::default();
        let key = Pubkey::new_unique();
        accounts.set_account(&key, Some(data.clone()));
        assert_eq!(accounts.get(&key).unwrap().value(), &data);

        accounts.set_account(&key, None);
        assert!(accounts.get(&key).is_none());
    }

    #[test]
    fn test_slot_history() {
        let accounts = AccountOverrides::default();
        let data = AccountSharedData::default();

        assert!(accounts.get(&sysvar::slot_history::id()).is_none());
        accounts.set_slot_history(Some(data.clone()));

        assert_eq!(
            accounts.get(&sysvar::slot_history::id()).unwrap().value(),
            &data
        );
    }
}
