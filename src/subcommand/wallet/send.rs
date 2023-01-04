use super::*;

#[derive(Debug, Parser)]
pub(crate) struct Send {
  address: Address,
  outgoing: Outgoing,
}

impl Send {
  pub(crate) fn run(self, options: Options) -> Result {
    let client = options.bitcoin_rpc_client_for_wallet_command("ord wallet send")?;

    if !self.address.is_valid_for_network(options.chain().network()) {
      bail!(
        "Address `{}` is not valid for {}",
        self.address,
        options.chain()
      );
    }

    let index = Index::open(&options)?;
    index.update()?;

    let unspent_outputs = get_unspent_outputs(&options)?;

    let inscriptions = index.get_inscriptions(None)?;

    let satpoint = match self.outgoing {
      Outgoing::SatPoint(satpoint) => {
        for inscription_satpoint in inscriptions.keys() {
          if satpoint == *inscription_satpoint {
            bail!("inscriptions must be sent by inscription ID");
          }
        }
        satpoint
      }
      Outgoing::InscriptionId(txid) => index
        .get_inscription_by_id(txid)?
        .map(|(_inscription, satpoint)| satpoint)
        .ok_or_else(|| anyhow!("No inscription found for {txid}"))?,
      Outgoing::Amount(amount) => {
        let all_inscription_outputs = inscriptions
          .keys()
          .map(|satpoint| satpoint.outpoint)
          .collect::<HashSet<OutPoint>>();

        let wallet_inscription_outputs = unspent_outputs
          .keys()
          .filter(|utxo| all_inscription_outputs.contains(utxo))
          .cloned()
          .collect::<Vec<OutPoint>>();

        if !client.lock_unspent(&wallet_inscription_outputs)? {
          bail!("failed to lock ordinal UTXOs");
        }

        let txid =
          client.send_to_address(&self.address, amount, None, None, None, None, None, None)?;

        println!("{txid}");

        return Ok(());
      }
    };

    let change = get_change_addresses(&options, 2)?;

    let unsigned_transaction = TransactionBuilder::build_transaction(
      satpoint,
      inscriptions,
      unspent_outputs,
      self.address,
      change,
    )?;

    let signed_tx = client
      .sign_raw_transaction_with_wallet(&unsigned_transaction, None, None)?
      .hex;

    let txid = client.send_raw_transaction(&signed_tx)?;

    println!("{txid}");

    Ok(())
  }
}
